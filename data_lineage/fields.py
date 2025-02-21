import re
import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import find_all_in_scope
from sqlglot.optimizer.scope import build_scope
import pandas as pd
from collections import defaultdict
from sqlglot.errors import OptimizeError
import os
from data_lineage.utils import measure_execution_time
from functools import wraps
import hashlib

# Dictionnaire de cache global
_analysis_cache = {}

def cache_analyze_projection(func):
    @wraps(func)
    def wrapper(projection, hql_content, results):
        # Générer une clé unique basée sur le HQL
        hql_hash = hashlib.md5(hql_content.encode('utf-8')).hexdigest()
        
        if hql_hash in _analysis_cache:
            return _analysis_cache[hql_hash]
        
        # Calcul de la projection et mise en cache
        result = func(projection, hql_content, results)
        _analysis_cache[hql_hash] = result
        return result
    
    return wrapper

def extract_table_names(query):
    """
    Recherche toutes les occurrences d'une clause du type :
    FROM "mon"."spark_ft_contract_snapshot" AS "alias"
    et renvoie la liste des noms de tables, au format mon.spark_ft_contract_snapshot.
    """
    # Cette regex capture :
    # 1) Le schéma ou database (tout ce qui est entre le premier pair de guillemets)
    # 2) Le nom de la table (entre le deuxième pair de guillemets)
    # 3) L'alias (entre le troisième pair de guillemets), même si on ne l'utilise pas ci-dessous.
    pattern = r'FROM\s+"([^"]+)"\."([^"]+)"\s+AS\s+"([^"]+)"'

    matches = re.findall(pattern, query, flags=re.IGNORECASE | re.DOTALL)

    # Exemple de match : [('mon', 'spark_ft_contract_snapshot', 'spark_ft_contract_snapshot'), ...]
    # On reforme le nom de la table "mon.spark_ft_contract_snapshot"
    table_names = [f"{schema}.{table}" for schema, table, alias in matches]

    table_names = set(table_names)

    return list(table_names)

def remove_comments(sql):
    # Supprime les commentaires de ligne (commençant par -- ou ---)
    sql = re.sub(r'--+.*?(\r\n|\r|\n)', '\n', sql)
    # Supprime les commentaires en bloc /* ... */
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql

def remove_hql_trim(hql_content):
    # Supprime les trim() vides
    hql_content = re.sub(r'trim\s*\(\s*\)', "''", hql_content, flags=re.IGNORECASE)
    return hql_content


def extract_lineage_fields(hive_sql):
    """
    
    pour une table en clé on a un liste de champs en valeurs
    """

    cleaned_hive_sql = remove_comments(hive_sql)
    cleaned_hive_sql= remove_hql_trim(cleaned_hive_sql)
    expression = sqlglot.parse_one(cleaned_hive_sql, read="hive")

    try:
        expression_qualified = qualify(expression)
    except OptimizeError as e:
        #print("Warning: Erreur d'optimisation capturée dans extract_lineage_fields :", e)
        expression_qualified = expression  # On continue avec l'expression non qualifiée
    
    root = build_scope(expression_qualified)
    # print("root",root)
    dic = {}

    for column in find_all_in_scope(root.expression, exp.Column):
        # print(root.sources)
        tables = extract_table_names(str(root.sources[column.table]))
        # print(f"coloumn : {str(column).split('.')[1]} => source: {extract_table_names(str(root.sources[column.table]))}")
        # print("")
        # Retirer les guillemets du champ
        # print("expre",str(column))
        a = str(column).split(".")[1].strip('"')
        # print("champs",a)
        for t in tables:
            # Si le nom de la table n'existe pas encore dans le dictionnaire,
            # on l'initialise à un set pour éviter les doublons.
            if t not in dic:
                dic[t] = set()
            dic[t].add(a)
    # Si vous souhaitez avoir des listes au final (au lieu de sets):
    for t in dic:
        dic[t] = list(dic[t])
    return dic
    


def extract_table_details_with_partition_and_if_not_exists(file_path):
    """
    Extrait le nom de la table, les noms des champs, les informations de partition
    et la présence de 'IF NOT EXISTS' d'une requête CREATE TABLE (ou CREATE EXTERNAL TABLE)
    dans un fichier .hql.

    Note : Dans cette version, tous les éléments (nom de table, noms de champs, clause de partition)
    sont renvoyés en majuscules. 'if_not_exists' reste un booléen.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            content = file.read()

        # 1) CREATE TABLE IF NOT EXISTS ... PARTITIONED BY ...
        create_table_match = re.search(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\S+)\s*\((.*?)\)\s*(PARTITIONED\s+BY\s*\(.*?\))?\s*STORED\s+AS\s+\w+",
            content,
            re.DOTALL | re.IGNORECASE,
        )

        if not create_table_match:
            # 2) CREATE TABLE ... PARTITIONED BY ... (sans IF NOT EXISTS)
            create_table_match = re.search(
                r"CREATE\s+TABLE\s+(\S+)\s*\((.*?)\)\s*(PARTITIONED\s+BY\s*\(.*?\))?\s*STORED\s+AS\s+\w+",
                content,
                re.DOTALL | re.IGNORECASE,
            )

            # 3) CREATE EXTERNAL TABLE (IF NOT EXISTS) ... LOCATION ...
            if not create_table_match:
                create_table_match = re.search(
                    r"CREATE\s+EXTERNAL\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)\s*\((.*?)\)\s*(COMMENT\s+'.*?')?\s*ROW\s+FORMAT\s+DELIMITED\s+FIELDS\s+TERMINATED\s+BY\s+'.*?'\s*LOCATION\s+'(.*?)'",
                    content,
                    re.DOTALL | re.IGNORECASE,
                )

                if not create_table_match:
                    print("Aucune requête CREATE (EXTERNAL) TABLE trouvée.")
                    return None, [], None, False

        # Déterminer si IF NOT EXISTS est présent dans le contenu (optionnel)
        if_not_exists = "IF NOT EXISTS" in content.upper()

        # Nom de la table (group(1))
        table_name = create_table_match.group(1)

        # Contenu entre parenthèses : colonnes (group(2))
        table_body = create_table_match.group(2)

        # Extraire les champs
        field_names = re.findall(r"\b(\w+)\b\s+\w+", table_body)

        # Par défaut, pas de partition
        partitioned_by = None

        # Vérifier la présence de PARTITIONED BY dans la capture group(3)
        if len(create_table_match.groups()) >= 3:
            group3 = create_table_match.group(3)
            if group3 and "PARTITIONED BY" in group3.upper():
                partitioned_by = group3

        # --- Convertir en MAJUSCULES ---
        # 1) Table name
        if table_name:
            table_name = table_name.upper()

        # 2) Field names
        field_names = [f.upper() for f in field_names]

        # 3) Partitioned by
        if partitioned_by:
            partitioned_by = partitioned_by.upper()

        # 4) if_not_exists reste un booléen, pas de conversion en majuscules
        return table_name, field_names, partitioned_by, if_not_exists

    except FileNotFoundError:
        print(f"Le fichier {file_path} est introuvable.")
        return None, [], None, False
    except Exception as e:
        print(f"Erreur : {e}")
        return None, [], None, False


def resolve_column_alias(column_name: str, dic_path: dict, results: dict) -> str:
    """
    dic_path(dict) 'dic_path' : dictionnaire des table->list champs pour la requête courante
    results(dict): dictionaire des provenant des requêtes create table, table->liste des champs

    :param column_name: ex: 'a.CHARGE' ou juste 'CHARGE'
    :param file_path: la clé pour accéder à results[file_path]
    :param results: dict de la forme
        results[file_path] = [
          {
            "table_name": "mon.spark_ft_contract_snapshot",
            "fields": ["charge", "main_credit", ...],
            ...
          },
          ...
        ]
    :return: ex: "mon.spark_ft_contract_snapshot.charge" si trouvé,
             ou la valeur d'origine si pas trouvé.
    """
    # 1) Isoler le col_name (sans alias)
    # print("column_name",column_name)
    split_col = column_name.split(".")
    # print("split_col:",split_col,"taille:",len(split_col))
    if len(split_col) == 2:
        # alias = 'a', col_name = 'CHARGE' (ex.)
        _, col_name = split_col
        return col_name
    elif len(split_col) == 3:
        col_name = split_col[2]
        return col_name
        # print("col_name",col_name)
    else:
        # Pas de point => la colonne est directe
        # (ex. col_name = 'CHARGE')
        # col_name=column_name
        pass

    # 2) Convertir en majuscules pour la comparaison

    for table_name, fields in dic_path.items():
        fields_upper = [f.upper() for f in fields]
        col_name = col_name.strip('`"').upper()
        # print("col name",col_name)
        if col_name in fields_upper:
            # print('operator_code true')
            return f"{col_name}"
        else:
            col_name = col_name.strip('`"').upper()
            for fp, table_info in results.items():
                if not isinstance(table_info, dict):
                    continue
                fields = table_info.get("fields", [])
                fields_upper = [f.upper() for f in fields]
                if col_name in fields_upper:
                    table_name = table_info.get(
                        "table_name", ""
                    ).upper()  # on met le nom de table en maj
                    if table_name and table_name in dic_path:
                        return f"{col_name}"
                    else:
                        continue

    return column_name

@cache_analyze_projection
def analyze_projection(projection: exp.Expression, hql_content: str, results: dict) -> dict:
    """
    Analyse une projection pour extraire :
      - columns_used : liste des colonnes (résolues si ambiguës) en minuscule
      - aggregations : liste des fonctions d'agrégation
      - arithmetic_ops : liste des opérations arithmétiques
      - formula_sql : la reconstitution de la projection en SQL
    """

    # Extraire la correspondance des tables et champs une seule fois
    dic_table_fields = extract_lineage_fields(hql_content)

    # Initialisation des listes
    columns_used = set()
    agg_funcs = set()
    arithmetic_ops = set()

    # Parcours des colonnes utilisées dans la projection
    for col in projection.find_all(exp.Column):
        table_part = f"{col.db}.{col.table}" if col.db else col.table or ""
        column_part = col.name
        raw_column_name = f"{table_part}.{column_part}" if table_part else column_part

        # Résolution des alias
        resolved = resolve_column_alias(raw_column_name, dic_table_fields, results)
        columns_used.add(resolved.lower())  # Conversion en minuscule pour la standardisation

    # Parcours des fonctions d'agrégation
    for func in projection.find_all(exp.AggFunc):
        func_name = func.__class__.__name__.upper()
        agg_funcs.add(func_name)

    # Détection des opérations arithmétiques
    ARITHMETIC_NODES = {exp.Add: "+", exp.Sub: "-", exp.Mul: "*", exp.Div: "/", exp.Mod: "%"}
    for node in projection.find_all(tuple(ARITHMETIC_NODES.keys())):
        op_symbol = ARITHMETIC_NODES[type(node)]
        arithmetic_ops.add(op_symbol)

    # Reconstitution de la projection en SQL
    formula_sql = projection.sql(dialect="hive")

    return {
        "columns_used": list(columns_used),
        "aggregations": list(agg_funcs),
        "arithmetic_ops": list(arithmetic_ops),
        "formula_sql": formula_sql,
    }


"""
def analyze_projection(projection: exp.Expression, hql_content: str, results: dict) -> dict:
    
    Analyse une projection pour extraire :
      - columns_used : liste des colonnes (résolues si ambiguës) en minuscule
      - aggregations : liste des fonctions d'agrégation
      - arithmetic_ops : liste des opérations arithmétiques
      - formula_sql : la reconstitution de la projection en SQL
    

    columns_used = []
    dic_table_fields = extract_lineage_fields(hql_content)
    for col in projection.find_all(exp.Column):
        if col.db:
            table_part = f"{col.db}.{col.table}" or ""
        else:
            table_part = col.table or ""

        column_part = col.name
        raw_column_name = f"{table_part}.{column_part}" if table_part else column_part
        # print("raw_column_name",raw_column_name)
       
        # print("raw_column_name")
        # Tenter de résoudre l'ambiguïté (ex. 'a.CHARGE' -> 'mon.spark_ft_contract_snapshot.charge')
        resolved = resolve_column_alias(raw_column_name, dic_table_fields, results)
        columns_used.append(resolved)

    # Fonctions d'agrégation
    agg_funcs = []
    for func in projection.find_all(exp.AggFunc):
        func_name = func.__class__.__name__.upper()
        if isinstance(func, exp.Count) and func.is_star:
            func_name = "COUNT(*)"
        agg_funcs.append(func_name)

    # Opérations arithmétiques
    arithmetic_ops = []
    ARITHMETIC_NODES = (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod)
    for node in projection.find_all(ARITHMETIC_NODES):
        arithmetic_ops.append(type(node).__name__.upper())

    formula_sql = projection.sql(dialect="hive")

    return {
        "columns_used": columns_used,
        "aggregations": agg_funcs,
        "arithmetic_ops": arithmetic_ops,
        "formula_sql": formula_sql,
    }
"""

def find_tables_in_select(select_expr: exp.Select) -> list:
    """
    Récupère toutes les tables mentionnées dans un SELECT donné
    (FROM, JOIN, etc.), sans doublons.
    Ex: ["mon.spark_ft_contract_snapshot", "dim.dt_zte_usage_type"]
    """
    tables = []
    for table_expr in select_expr.find_all(exp.Table):
        #print("table_expr", table_expr)
        if table_expr.db:
            #print("db", table_expr.db)
            tables.append(f"{table_expr.db}.{table_expr.name}")
        else:
            tables.append(table_expr.name)
    return list(set(tables))


def get_col_name_without_table(col_name: str) -> str:
    """ """
    # 1) Remove outer quotes
    col_no_quotes = col_name.replace('"', "")
    # e.g. "ft_contract_snapshot"."operator_code" -> ft_contract_snapshot.operator_code

    # 2) Split on the first dot (if any)
    parts = col_no_quotes.split(".", 1)
    # e.g. ["ft_contract_snapshot", "operator_code"]
    # print("parts",parts)
    if len(parts) == 2:
        return parts[1]
    else:
        return parts[0]


def create_lineage_dic(hql_file_path: str, results: dict) -> dict:
    """
    Lit une requête HQL depuis un fichier,
    parse et qualifie la requête, puis construit un dictionnaire
    de la forme :
      {
        "<chemin_fichier>.hql": {
          "ALIAS_OR_NAME": {
            "Alias/Projection": ...,
            "Colonnes détectées": [...],
            "Fonctions d'agg": [...],
            "Opérations arithmétiques": [...],
            "Formule SQL": ...,
            "Table(s) utilisées": ...
          },
          ...
        }
      }
    """
    lineage_dict = {}
    temp_projection=0

    try:
        with open(hql_file_path, "r", encoding="utf-8") as f:
            hql_content = f.read()
    except FileNotFoundError:
        print(f"Fichier introuvable: {hql_file_path}")
        return {}
    
    hql_content=remove_comments(hql_content)
    hql_content=remove_hql_trim(hql_content)
    expression = sqlglot.parse_one(hql_content, read="hive")
    if not expression:
        print(f"Impossible de parser le HQL dans: {hql_file_path}")
        return {}
    
    try:
        expression_qualified= qualify(expression)
    except sqlglot.errors.OptimizeError as e:
        print(f"Warning: {e}")  # Affiche un avertissement sans interrompre l'exécution
        expression_qualified = expression  
    all_selects = list(expression_qualified.find_all(exp.Select))
    print("file_path",hql_file_path)
    lineage_dict[hql_file_path] = {}
    for select_expr in all_selects:
        tables_in_select,_ = measure_execution_time(find_tables_in_select,select_expr)
        # print("tablein select",tables_in_select)
        tables_str = ", ".join(tables_in_select) if tables_in_select else "Aucune table"
        for proj in select_expr.selects:
            if isinstance(proj, exp.Alias):
                alias_name = proj.alias or "NO_ALIAS"
                expr_to_analyze = proj.this
                # print("expr to analyze",expr_to_analyze)
                # print(repr(proj))

            else:
                alias_name = proj.alias_or_name or "NO_ALIAS"
                expr_to_analyze = proj
            info,t = measure_execution_time(analyze_projection,expr_to_analyze, hql_content, results)
            temp_projection+=t

            lineage_dict[hql_file_path][alias_name] = {
                "Alias/Projection": alias_name,
                "Colonnes détectées": info["columns_used"],
                # "Schema": schemas_par_col,
                "agg": info["aggregations"],
                "Opérations arithmétiques": info["arithmetic_ops"],
                "Formule SQL": info["formula_sql"],
                "Table(s) utilisées": tables_str,
            }

    print("analyse des champs",temp_projection)

    return lineage_dict


def create_dict_tables_dependencies_and_path(
    dict_table_paths: dict,
    dic_rdms_hive_dependencies: dict,
    dict_rdms_fields: dict
) -> dict:
    """
    Crée un dictionnaire des dépendances de tables RDMS et associe chaque dépendance à son fichier HQL.
    
    Args:
        dict_table_paths (dict): Dictionnaire associant chaque table Hive à son fichier HQL.
                                 Exemple: {"table_hive1": "chemin/vers/fichier1.hql", ...}
        dic_rdms_hive_dependencies (dict): Dictionnaire des dépendances RDMS -> Hive.
                                           Exemple: {"rdms_table": {"dependencies": ["hive_table1", "hive_table2"]}, ...}
        dict_rdms_fields (dict): Dictionnaire associant une table RDMS à la liste de ses champs.
                                 Exemple: {"rdms_table": ["champ1", "champ2", "champ3"], ...}
    
    Returns:
        dict: Dictionnaire structuré sous la forme:
              {
                  "rdms_table1": {
                      "liste_champs": ["champ1", "champ2"],
                      "dependencies": {
                          "hive_table1": "file1.hql",
                          "hive_table2": "file2.hql"
                      }
                  },
                  "rdms_table2": {
                      "liste_champs": ["champA", "champB"],
                      "dependencies": {
                          "hive_table3": "file3.hql"
                      }
                  }
              }
    """
    dict_tables_dependencies = {}
    for rdms, value in dic_rdms_hive_dependencies.items():
        dependencies = value.get('dependencies', [])
        second_dependency=None
         #on récupère la première dependance ton récupère ses champs
        first_dependency=None
        second_dependency=dependencies[1]
        first_dependency=dependencies[0]
        
        fields=[]
        for i,value in dict_rdms_fields.items():
                table_name=value.get('table_name',None)
                if table_name!=None and table_name==second_dependency:
                        fields=value.get('fields',[])
                        

         # Initialisation de l'entrée pour cette table RDMS
        dict_tables_dependencies[rdms] = {
            "rdms_table":first_dependency,
            "first_hive table":second_dependency,
            "liste_champs": fields,  # Récupération des champs RDMS
            "dependencies": {}
        }
        for dep in range(0,len(dependencies)):
            # Récupérer le fichier HQL associé à la table dépendante

            # on stocke les informations de toutes les autres tables dependantes
            if dep >0:
                # récupération paths 
                file_dep = dict_table_paths.get(dependencies[dep], None)
                # Stocker la dépendance avec son fichier HQL
                dict_tables_dependencies[rdms]["dependencies"][dependencies[dep]] = file_dep
    return dict_tables_dependencies


def build_lineage(dependencies, results):
    """
    Construit le lignage des tables Hive à partir des fichiers HQL.

    Args:
        dependencies (dict): Dictionnaire des dépendances où chaque clé est une table Hive
                             et chaque valeur est une liste de fichiers HQL associés.
        results (dict): Dictionnaire contenant des résultats intermédiaires pouvant être utilisés
                        par la fonction `create_lineage_dic`.

    Returns:
        dict: Dictionnaire des lignages où chaque clé est un fichier HQL et chaque valeur est
              le résultat de l'analyse par `create_lineage_dic`.
    """
    lineage = {}
    for hive_table, hql_files in dependencies.items():
        if hive_table==None or hql_files==None:
            return {}
        if isinstance(hql_files, str):  # Gérer le cas où un seul fichier est donné sous forme de chaîne
            hql_files = [hql_files]        
        for hql_file in hql_files:
            if not hql_file.startswith("/"):
                if os.path.exists(hql_file):  # Vérifie que le fichier existe
                    lineage[hql_file] = create_lineage_dic(hql_file, results)
                else:
                    print(f"Fichier HQL non trouvé : {hql_file}")
    return lineage

def track_fields_across_lineage(data, results):
    """
    Suit l'origine des champs (`liste_champs`) en fonction des lignages des tables Hive.

    Args:
        data (dict): Dictionnaire contenant plusieurs tables RDMS et leurs informations :
                     - "liste_champs" : Liste des champs à suivre
                     - "dependencies" : Dictionnaire des tables Hive et leurs fichiers HQL associés
        results (dict): Dictionnaire contenant des résultats intermédiaires pour l'analyse.

    Returns:
        dict: Dictionnaire contenant le lignage des champs sous la forme :
              {
                  "rdms_table1": {
                      "champ1": ["table1", "table2"],
                      "champ2": ["table3"]
                  },
                  "rdms_table2": {
                      "champA": ["table4"]
                  }
              }
    """
    overall_field_tracking = defaultdict(lambda: defaultdict(list))  # Dictionnaire imbriqué {rdms_table -> {champ -> [tables]}}

    for rdms_table, table_data in data.items():
        liste_champs = table_data.get("liste_champs", [])
        dependencies = table_data.get("dependencies", {})

        lineage = build_lineage(dependencies, results)  # Extraction du lignage pour cette table
        
        for field in liste_champs:
            for hql_file, tables in lineage.items():
                for table, details in tables.items():
                    if any(field in details.get("Colonnes détectées", []) for details in tables.values()):
                        overall_field_tracking[rdms_table][field].append(table)

    return overall_field_tracking


def get_unique_tables_names_from_lineage_dict(lineage_dict:dict)->list:
    """
    Extrait les noms uniques des tables à partir d'un dictionnaire de lignage de données.
    Args:
        lineage_dict (dict): Dictionnaire contenant les informations de lignage des fichiers HQL.
                             Il est structuré sous la forme :
                             {
                                 "chemin_du_fichier.hql": {
                                     "alias_1": {"Table(s) utilisées": "nom_table"},
                                     "alias_2": {"Table(s) utilisées": "autre_table"},
                                     ...
                                 },
                                 ...
                             }
    Returns:
        list
    """
    set_tables=set()
    for hql_path, aliases_info in lineage_dict.items():
        #print(f"\n=== FICHIER HQL : {hql_path} ===")
        for alias_name, details in aliases_info.items():
            table_name=details.get('Table(s) utilisées',None)
            table_name=table_name.upper()
            set_tables.add(table_name)
    #set_tables.add(lineage_dict.get("Table(s) utilisées",None))
        
    return list(set_tables)

def get_hql_path_from_table_name(dict_table_paths:dict,table_names:list)->dict:
    list_paths=[]
    dic_table_hql_paths={}
    for i in table_names:
        hql_path=dict_table_paths.get(i,None)
        dic_table_hql_paths[i]={'list_hql':hql_path}
    return dic_table_hql_paths

def get_table_paths(list_tables,dic_rdms_hive,dic_hive_depencies):
    """
    génère un dictionnaire où pour chaque table contenu dans une 
    ligne de dependances on a son où ses fichiers de création 
    """
    

def print_lineage_dict(lineage_dict: dict):
    """
    Affiche le dictionnaire de lineage de manière lisible.
    """
    for hql_path, aliases_info in lineage_dict.items():
        print(f"\n=== FICHIER HQL : {hql_path} ===")
        for alias_name, details in aliases_info.items():
            print(f"  - Alias/Projection : {alias_name}")
            print(f"    Colonnes détectées       : {details['Colonnes détectées']}")
            print(f"    Fonctions d'agg          : {details['agg']}")
            print(f"    Opérations arithmétiques : {details['Opérations arithmétiques']}")
            print(f"    Formule SQL              : {details['Formule SQL']}")
            print(f"    Tables utilisées       : {details['Table(s) utilisées']}")
            print()


def export_lineage_to_excel(lineage_dict: dict, output_excel_path: str):
    """
    Exporte le dictionnaire de lineage dans un fichier Excel.

    :param lineage_dict: dict, résultat de create_lineage_dic
    :param output_excel_path: str, chemin du fichier Excel de sortie
    """
    # Liste pour stocker les lignes de l'Excel
    excel_rows = []
    for hql_path, aliases_info in lineage_dict.items():
        for alias_name, details in aliases_info.items():
            row = {
                "Nom du Fichier": hql_path,
                "Alias/Projection": alias_name,
                "Schema": details.get("Schema", ""),
                "Colonnes détectées": ", ".join(details.get("Colonnes détectées", [])),
                "agg": ", ".join(details.get("agg", [])),
                "Opérations arithmétiques": ", ".join(details.get("Opérations arithmétiques", [])),
                "Formule SQL": details.get("Formule SQL", ""),
                "Tables utilisées": details.get("Table(s) utilisées", ""),
            }
            excel_rows.append(row)

    # Créer un DataFrame pandas
    df = pd.DataFrame(
        excel_rows,
        columns=[
            "Nom du Fichier",
            "Alias/Projection",
            "Colonnes détectées",
            "agg",
            "Opérations arithmétiques",
            "Formule SQL",
            "Tables utilisées",
        ],
    )
    # Exporter le DataFrame vers Excel
    try:
        df.to_excel(output_excel_path, index=False)
        print(f"Les résultats ont été exportés avec succès vers {output_excel_path}")
    except Exception as e:
        print(f"Erreur lors de l'exportation vers Excel: {e}")


def process_hql_files(file_paths):
    """
    Traite une liste de chemins de fichiers HQL pour extraire le nom de la table et ses champs,
    uniquement pour les fichiers dont le nom contient "create".

    Args:
        file_paths (list): Liste des chemins de fichiers HQL.

    Returns:
        dict: Dictionnaire avec en clé le chemin du fichier HQL et en valeur le nom de la table et une liste de ses champs.
    """
    results = {}

    for file_path in file_paths:
        # Vérification du nom du fichier contenant "create" et de l'extension .hql
        if file_path.endswith(".hql") and "create" in file_path.lower():
            try:
                # Extraction du nom de la table et des champs
                table_name, fields_list, partitioned_by, if_not_exists = (
                    extract_table_details_with_partition_and_if_not_exists(file_path)
                )
                results[file_path] = {
                    "table_name": table_name,
                    "fields": fields_list,
                    "partitioned_by": partitioned_by,
                    "if_not_exists": if_not_exists,
                }
            except ValueError as e:
                print(f"Erreur lors du traitement du fichier {file_path}: {e}")
        else:
            print(
                f"Le fichier {file_path} n'est pas un fichier HQL ou ne contient pas de requête CREATE TABLE."
            )

    return results
