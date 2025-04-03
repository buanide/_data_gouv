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


_analysis_cache = {}

def cache_analyze_projection(func):
    """
    Décorateur pour mettre en cache les résultats de l'analyse des projections SQL.

    Ce décorateur utilise un cache pour stocker les résultats de l'analyse des projections SQL
    afin d'éviter de recalculer les mêmes résultats pour les mêmes projections. Le cache est basé
    sur un hachage MD5 du contenu HQL et de la projection SQL.

    Args:
        func (function): La fonction d'analyse de projection à décorer.

    Returns:
        function: La fonction décorée avec mise en cache.

    Exemple:
        @cache_analyze_projection
        def analyze_projection(projection, hql_content, results):
            # Analyse de la projection
            ...

    Notes:
        - Le cache est stocké dans la variable `_analysis_cache`.
        - Le hachage MD5 est utilisé pour générer des clés uniques pour le contenu HQL et les projections SQL.
    """
    @wraps(func)
    def wrapper(projection, hql_content, results):
        hql_hash = hashlib.md5(hql_content.encode('utf-8')).hexdigest()
        
        # Initialiser le cache pour ce HQL s'il n'existe pas
        if hql_hash not in _analysis_cache:
            _analysis_cache[hql_hash] = {}

        # Générer un identifiant unique pour la projection (hash de son SQL)
        projection_sql = projection.sql(dialect="hive")
        projection_hash = hashlib.md5(projection_sql.encode('utf-8')).hexdigest()

        # Vérifier si la projection est déjà analysée
        if projection_hash in _analysis_cache[hql_hash]:
            return _analysis_cache[hql_hash][projection_hash]

        # Calcul de l'analyse et stockage
        result = func(projection, hql_content, results)
        _analysis_cache[hql_hash][projection_hash] = result
        return result
    
    return wrapper



def extract_table_names(query):
    """
    Searches for all occurrences of a clause of the type:
    FROM "schema"."table" AS "alias"
    and returns a list of table names in the format schema.table.

    Args:
        query (str): The SQL query to analyze.

    Returns:
        list: A list of table names found in the query.
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
    """
    Removes line and block comments from an SQL query.

    Args:
        sql (str): The SQL content to clean.

    Returns:
        str: The SQL content without comments.
    """
    # Supprime les commentaires de ligne (commençant par -- ou ---)
    sql = re.sub(r'--+.*?(\r\n|\r|\n)', '\n', sql)
    # Supprime les commentaires en bloc /* ... */
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql

def remove_hql_trim(hql_content):
    """
    Removes empty trim() functions from an HQL query.

    Args:
        hql_content (str): The HQL content to clean.

    Returns:
        str: The HQL content without empty trim() functions.
    """
    # Supprime les trim() vides
    hql_content = re.sub(r'trim\s*\(\s*\)', "''", hql_content, flags=re.IGNORECASE)
    return hql_content


def extract_lineage_fields(hive_sql):
    """
    Extracts the lineage fields for a table in a Hive SQL query.

    Args:
        hive_sql (str): The Hive SQL query to analyze.

    Returns:
        dict: A dictionary where the key is the table name and the value is a list of fields.
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
        table_name=column.table

        if not table_name:
            table_name = "UNKNOWN_TABLE"
            #print("unknwo table pour colonne",column)
   
        #root.sources est un dictionnaire qui associe les noms de tables aux sources SQL correspondantes.
        if table_name in root.sources:
            tables = extract_table_names(str(root.sources[table_name]))
            a = str(column).split(".")[1].strip('"')
        else:
            tables = [table_name]
            a=column.name
        # print(f"coloumn : {str(column).split('.')[1]} => source: {extract_table_names(str(root.sources[column.table]))}")
        # print("")
        # Retirer les guillemets du champ
        
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
    Extracts the table name, field names, partition information,
    and the presence of 'IF NOT EXISTS' from a CREATE TABLE (or CREATE EXTERNAL TABLE) query
    in an .hql file.

    Args:
        file_path (str): The path to the HQL file to analyze.

    Returns:
        tuple: Contains the table name, a list of field names, partition information, and a boolean indicating the presence of 'IF NOT EXISTS'.
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

        if table_name:
            table_name = table_name.upper()

        field_names = [f.upper() for f in field_names]

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
    Resolves column aliases in an SQL query.

    Args:
        column_name (str): The name of the column to resolve.
        dic_path (dict): Dictionary of tables and their fields for the current query, created from the HQL file using the 'extract_lineage_fields' function.
        results (dict): Dictionary of fields from CREATE TABLE queries. (results[file_path] = [
          {
            "table_name": "mon.spark_ft_contract_snapshot",
            "fields": ["charge", "main_credit", ...],
            ...
          },
          ...
        ])

    Returns:
        str: The resolved column name. ex: "mon.spark_ft_contract_snapshot.charge" if founded, if not it returns the
             the original value .
    """

   
    # 1) Isoler le col_name (sans alias)
    # print("column_name",column_name)
    split_col = column_name.split(".")
  
   
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
        col_name=column_name
        pass

    # 2) Recherche du nom du champs dans le dictionnaire où on table->liste_champs pour le fichier .hql en cours de traitement
    # sinon on recherche dans le dictionnaire des table->liste champs créé à partir des requêtes CREATE TABLE 
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

    #print("dic_table_fields",dic_table_fields)
    # Initialisation des listes
    columns_used = []
    agg_funcs = set()
    arithmetic_ops = set()

    #print("projection",projection)
    # Parcours des colonnes utilisées dans la projection
    for col in projection.find_all(exp.Column):
        #print("col",col)
        table_part = f"{col.db}.{col.table}" if col.db else col.table or ""
        column_part = col.name
        #print('col.db',col.db,'col.table',col.table,'col.name',col.name)
        raw_column_name = f"{table_part}.{column_part}" if table_part else column_part
        #print("raw_column_name",raw_column_name)
        # Résolution des alias
        resolved = resolve_column_alias(raw_column_name, dic_table_fields, results)
        columns_used.append(resolved)  # Conversion en minuscule pour la standardisation
        

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



def create_lineage_dic(hql_file_path: str, results: dict) -> dict:
    """
        Lit une requête HQL depuis un fichier, parse et qualifie la requête, puis construit un dictionnaire
        de lignage des données.

        Cette fonction analyse une requête HQL pour extraire les informations de lignage des données, telles que
        les colonnes détectées, les fonctions d'agrégation, les opérations arithmétiques, la formule SQL et les
        tables utilisées. Le résultat est structuré sous la forme d'un dictionnaire.

        Args:
            hql_file_path (str): Le chemin du fichier HQL à analyser.
            results (dict): Dictionnaire contenant des résultats intermédiaires pour l'analyse, issu de la fonction 'process_hql_files' 

        Returns:
            dict: Dictionnaire des lignages où chaque clé est un fichier HQL et chaque valeur est un dictionnaire
                contenant les informations de lignage des données. La structure est la suivante :
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
                    },
                    ...
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
            
            #print("expr to analyze",expr_to_analyze)
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
    #print("lineage_dict",lineage_dict)
    return lineage_dict


def create_dict_tables_dependencies_and_path(
    dict_table_paths: dict,
    dic_rdms_hive_dependencies: dict,
    dict_rdms_fields: dict,
    dict_file_queries:dict
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
        dict_file_queries(dict): Dictionnaire contenant les infos associé à une table rdms

        Exemple:{'pre_exec': [], 'exec': [], 'raw_directorirectory': None, 
        'tt_directory': None, 'cdr_tt': None, 'staging_table_dwh': 'MON.SQ_TMP_SVI_APPEL_SELFCARE'}


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
        staging_table_dwh=None
        dwh_table=None
        second_dependency=dependencies[1]
        first_dependency=dependencies[0]
        
        fields=[]
        for i,value in dict_rdms_fields.items():
                table_name=value.get('table_name',None)
                if table_name!=None and table_name==second_dependency:
                        fields=value.get('fields',[])
                        
        for i,value in dict_file_queries.items():
            dwh_table=value.get('dwh_table')
            if dwh_table!=None:
                #print("dwh_table",dwh_table)
                if first_dependency.upper()==dwh_table.upper():
                    staging_table_dwh=value.get("staging_table_dwh",None)
                    break
                   

        
         # Initialisation de l'entrée pour cette table RDMS
        dict_tables_dependencies[rdms] = {
            "rdms_table":first_dependency,
            "first_hive table":second_dependency,
            "liste_champs": fields,  # Récupération des champs RDMS
            "staging_table_dwh":staging_table_dwh,
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


def create_dict_tables_dependencies_and_path_for_hive_tables(
    dict_table_paths: dict,
    dic_hive_dependencies: dict,
    dict_rdms_fields: dict
) -> dict:
    """
    Crée un dictionnaire des dépendances de tables Hive et associe chaque dépendance à son fichier HQL.
    
    Args:
        dict_table_paths (dict): Dictionnaire associant chaque table Hive à son fichier HQL.
                                 Exemple: {"table_hive1": "chemin/vers/fichier1.hql", ...}
        dic_hive_dependencies (dict): Dictionnaire des dépendances Hive -> Hive.
                                      Exemple: {"hive_table": {"dependencies": ["hive_table1", "hive_table2"]}, ...}
        dict_rdms_fields (dict): Dictionnaire associant une table RDMS à la liste de ses champs.
                                 Exemple: {"rdms_table": ["champ1", "champ2", "champ3"], ...}
    
    Returns:
        dict: Dictionnaire structuré sous la forme:
              {
                  "hive_table1": {
                      "hive_table1": ["file1.hql"],
                      "hive_table2": ["file2.hql"],
                      "hive_table3": ["file3.hql"]
                  },
                  "hive_table2": {
                      "hive_table2": ["file2.hql"],
                      "hive_table4": ["file4.hql"]
                  }
              }
    """
    dict_tables_dependencies = {}

    # Parcourir les dépendances hive -> Hive


    for table, value in dic_hive_dependencies.items():
        dict_tables_dependencies[table] = {}
        dependencies = value.get('dependances', [])
        
        # Ajouter la table elle-même à ses dépendances
        if table not in dict_tables_dependencies[table]:
            dict_tables_dependencies[table][table] = []
        
        # Récupérer le fichier HQL associé à la table elle-même
        file_self = dict_table_paths.get(table, None)
        if file_self:
            dict_tables_dependencies[table][table].append(file_self)

        for dep in dependencies:
            if dep not in dict_tables_dependencies[table]:
                dict_tables_dependencies[table][dep] = []
                
            # Récupérer le fichier HQL associé à la table dépendante
            file_dep = dict_table_paths.get(dep, None)
            if file_dep:
                dict_tables_dependencies[table][dep].append(file_dep)

    # Convertir les listes de listes en listes simples
    for table, deps in dict_tables_dependencies.items():
        for dep, paths in deps.items():
            # Aplatir les listes de listes en une seule liste
            flat_list = []
            for path in paths:
                if isinstance(path, list):
                    flat_list.extend(path)
                else:
                    flat_list.append(path)
            dict_tables_dependencies[table][dep] = list(set(flat_list))

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
        if hive_table!=None:

            if isinstance(hql_files, str):  # Gérer le cas où un seul fichier est donné sous forme de chaîne
                hql_files = [hql_files] 
            if hql_files!=None:       
                for hql_file in hql_files:
                    if not hql_file.startswith("/"):
                        if os.path.exists(hql_file):  # Vérifie que le fichier existe
                            current_lineage_dict=create_lineage_dic(hql_file, results)
                            #print("current_lineage_dict",current_lineage_dict)
                            lineage[hql_file] = current_lineage_dict
                            #print("lineage",lineage)
                        else:
                            print(f"Fichier HQL non trouvé : {hql_file}")
            else:
                pass
    return lineage


    

def track_fields_across_lineage(rdms_table_name,data, results,dic_fields_from_dwh):
    """
    Suit les opérations menés sur les colonnes de la première à la dernière table pour chaque ligne de dépendances  pour une table rdms

    Args:
        data (dict): Dictionnaire contenant plusieurs tables RDMS et leurs informations :
                     - "liste_champs" : Liste des champs à suivre
                     - "dependencies" : Dictionnaire des tables Hive et leurs fichiers HQL associés
        results (dict): Dictionnaire contenant des résultats intermédiaires pour l'analyse.

    Returns:
        dict: Dictionnaire contenant le lignage des champs sous la forme :
              {
                  "champ1": [
                      { "chemin_du_fichier.hql": "path/alors/exec.hql",
                        "Opérations arithmétiques": ["+", "-", ...],
                        "Formule SQL": "SELECT ... FROM ... WHERE ...",
                        "Table(s) utilisées": ["table1", "table2"]},
                      { ... }
                  ],
                  "champ2": [ ... ]
              }
    """
    overall_field_tracking = {}
    # parcours du dictionnaire contenant le sinfos des tables rdms


    for i, info in data.items():
        fields_first_hive_table = info.get("liste_champs", [])
        rdms=info.get('rdms_table')
        tmp_dwh=info.get('staging_table_dwh',None)
        first_hive_table=info.get('first_hive table')
        #print('rmds_table',rdms)
        fields_rdms_tmp=None
        
        #print("rdms_table_name",rdms_table_name)
        if rdms.lower()==rdms_table_name.lower():
            # on extrait les dependences du datalake des tables rdms
            dependencies = info.get("dependencies",None)
            lineage = build_lineage(dependencies, results)  # Extraction du dictionnaire de lineag epour cette table
            # pour chaque fichier hql correspondant à l'alimentation d'une table on a besoin des informations sur chacun des champs de cette table sous forme d'un dictionnaire
            #print("lineage",lineage)
            
        # Recherche des champs de la table temporaire et rdms finale dans le dictionnaire en paramètre dans le dictionnaire et on récupère ses champs 
            for i,value in dic_fields_from_dwh.items():
                    if i.lower()==tmp_dwh.lower():
                        fields_rdms_tmp=value
                        #print("rdms_temp_fields",fields_rdms_tmp)

                    if i.lower()==rdms.lower():
                        fields_rdms=value

                    if fields_rdms_tmp!=None and fields_rdms!=None:
                        break

            if fields_rdms_tmp!=None:
                    # 
                    for hql_file, tables in lineage.items():
                        for table, details in tables.items():
                            for key, info in details.items():
                                detected_column = info.get("Colonnes détectées",None)
                                if not detected_column:  # Si aucune colonne détectée
                                    detected_column = "NO DETECTED COLUMN"
                                    if not detected_column:
                                        detected_column = "INCONNUE"
                                # Si c'est une liste, on la met en minuscule
                                if isinstance(detected_column, list):
                                    detected_column = [col.lower() for col in detected_column]
                                else:
                                    detected_column = detected_column.lower()
                                for col in detected_column if isinstance(detected_column, list) else [detected_column]:
                                    if col not in overall_field_tracking:
                                        overall_field_tracking[col] = []
                                    # on a besoin de connaitre à quel champ de la table temporaire au dwh correspond le champ de la table du datalake
                                    alias=info.get("Alias/Projection", None)
                                    alias_upper=alias.upper()
                                    #print("fields_rdms_tmp",fields_rdms_tmp)
                                    previous_entry = None
                                    if alias!=None:
                                        # on regarde si l'alias est dans la liste des champs des champs
                                        #  de dernière table d'aggrégation avant l'insertion dans la table rdms     
                                        if  alias_upper in fields_rdms_tmp:
                                            try:    
                                               # on se rassure que les deux listes de champs ont la même taille 
                                              
                                                if len(fields_rdms_tmp)==len(fields_rdms):
                                                     #print("same size")
                                                     indice = fields_rdms_tmp.index(alias_upper)  # 25 n'est pas dans la liste
                                                     rdms_field=fields_rdms[indice]
                                                     #print("rdms_field",rdms_field)
                                                     #print("alias",alias)
                                                     field_entry = {
                                                        "rdms_field":rdms_field,
                                                        "path": "",
                                                        "colonne": "",
                                                        "Opérations arithmétiques": "",
                                                        "Alias": alias,
                                                        "Formule SQL": "",
                                                        "Table(s) utilisées": ""
                                                    }
                                                     overall_field_tracking[col].append(field_entry)
                                            except ValueError:
                                                print("L'alias n'est pas dans la liste des champs de la table")
                                        
                                        formule=info.get("Formule SQL", "")
                                        if col in formule:
                                            if rdms_field!=None:
                                                field_entry = {
                                                    "rdms_field":rdms_field,
                                                    "path": hql_file,
                                                    "colonne": col,
                                                    "Opérations arithmétiques": info.get("Opérations arithmétiques", []),
                                                    "Alias": info.get("Alias/Projection", None),
                                                    "Formule SQL": info.get("Formule SQL", ""),
                                                    "Table(s) utilisées": info.get("Table(s) utilisées", "")
                                                }
                                            else:
                                                field_entry = {
                                                    "rdms_field":"",
                                                    "path": hql_file,
                                                    "colonne": col,
                                                    "Opérations arithmétiques": info.get("Opérations arithmétiques", []),
                                                    "Alias": info.get("Alias/Projection", None),
                                                    "Formule SQL": info.get("Formule SQL", ""),
                                                    "Table(s) utilisées": info.get("Table(s) utilisées", "")
                                                }

                                    overall_field_tracking[col].append(field_entry)
    return overall_field_tracking


def track_fields_across_lineage_for_data_lake(data_lake_table, data, results, dic_hive_dependencies):
    """
    Suit les opérations menées sur les colonnes d'une table du datalake donnée pour chaque ligne de dépendances.
    
    Args:
        data_lake_table (str): Le nom de la table du datalake à suivre.
        data (dict): Dictionnaire contenant plusieurs tables RDMS et leurs informations :
                     - "liste_champs" : Liste des champs à suivre
                     - "dependencies" : Dictionnaire des tables Hive et leurs fichiers HQL associés
        results (dict): Dictionnaire contenant des résultats intermédiaires pour l'analyse.
        dic_hive_dependencies (dict): Dictionnaire des dépendances Hive -> Hive.
                                      Exemple: {"hive_table": {"dependencies": ["hive_table1", "hive_table2"]}, ...}

    Returns:
        dict: Dictionnaire contenant le lignage des champs sous la forme :
              {
                  "champ1": [
                      { "chemin_du_fichier.hql": "path/alors/exec.hql",
                        "Opérations arithmétiques": ["+", "-", ...],
                        "Formule SQL": "SELECT ... FROM ... WHERE ...",
                        "Table(s) utilisées": ["table1", "table2"]},
                      { ... }
                  ],
                  "champ2": [ ... ]
              }
    """
    overall_field_tracking = {}

    # Parcours du dictionnaire des tables RDMS et de leurs dépendances
    for i, info in data.items():
        # Recherche de la table du datalake en paramètre dans le dictionnaire
        table_hive = info.get('first_hive table', None)
        
        # Cas où la table du datalake est trouvée dans la première dépendance
        if table_hive and table_hive.lower() == data_lake_table.lower():
            dependencies = info.get("dependencies", None)
            if dependencies:
                lineage = build_lineage(dependencies, results)  # Extraction du lignage pour cette table
                overall_field_tracking.update(process_lineage(lineage))

        else:
            # Cas où la table Hive se trouve dans les autres dépendances
            dependencies = info.get("dependencies", None)
            if dependencies:
                for dep, path in dependencies.items():
                    if dep!=None:
                        if dep.lower() == data_lake_table.lower():
                            lineage = build_lineage(dependencies, results)
                            overall_field_tracking.update(process_lineage(lineage))

    # Si la table n'est pas trouvée dans le dictionnaire `data`, chercher dans `dic_hive_dependencies`
    if not overall_field_tracking:
        for table, value in dic_hive_dependencies.items():
            if table.lower() == data_lake_table.lower():
                if value!=None:
                    lineage = build_lineage(value, results)
                    overall_field_tracking.update(process_lineage(lineage))
                    break

    return overall_field_tracking

def process_lineage(lineage):
    """
    Traite le lignage pour extraire les informations de suivi des champs.

    Args:
        lineage (dict): Dictionnaire des lignages où chaque clé est un fichier HQL et chaque valeur est
                        le résultat de l'analyse par `create_lineage_dic`.

    Returns:
        dict: Dictionnaire contenant le suivi des champs.
    """
    field_tracking = {}

    for hql_file, tables in lineage.items():
        for table, details in tables.items():
            for key, info in details.items():
                detected_column = info.get("Colonnes détectées", None)
                if not detected_column:  # Si aucune colonne détectée
                    detected_column = "NO DETECTED COLUMN"
                    if not detected_column:
                        detected_column = "INCONNUE"

                # Si c'est une liste, on la met en minuscule
                if isinstance(detected_column, list):
                    detected_column = [col.lower() for col in detected_column]
                else:
                    detected_column = detected_column.lower()

                for col in detected_column if isinstance(detected_column, list) else [detected_column]:
                    if col not in field_tracking:
                        field_tracking[col] = []

                    field_entry = {
                        "path": hql_file,
                        "colonne": col,
                        "Opérations arithmétiques": info.get("Opérations arithmétiques", []),
                        "Alias": info.get("Alias/Projection", None),
                        "Formule SQL": info.get("Formule SQL", ""),
                        "Table(s) utilisées": info.get("Table(s) utilisées", "")
                    }

                    field_tracking[col].append(field_entry)

    return field_tracking



def export_tracking_lineage_to_excel(lineage_data, file_name):
    """
    Exporte le lineage des champs sous forme d'un fichier Excel.

    Args:
        lineage_data (dict): Résultat de `track_fields_across_lineage`
        file_name (str): Nom du fichier Excel de sortie (par défaut "lineage_tracking.xlsx")
    """
    all_data = []
    for field, entries in lineage_data.items():
        for entry in entries:
            all_data.append({
                "Tables utilisées": entry.get("Table(s) utilisées", ""),
                "dwh_fields": entry.get("rdms_field", ""),
                "Champ": entry.get("colonne", ""),
                "Alias":entry.get("Alias",""),
                "Chemin du fichier HQL": entry["path"],
                "Opérations arithmétiques": ", ".join(entry["Opérations arithmétiques"]),
                "Formule SQL": entry["Formule SQL"]     
            })

    df = pd.DataFrame(all_data)
    df= df.drop_duplicates()
    # Exporter vers Excel
    df.to_excel(file_name, index=False, engine="openpyxl")


def export_tracking_lineage_to_excel_2(lineage_data, file_name):
     """
     Exporte le lineage des champs sous forme d'un fichier Excel, en regroupant par dwh_fields
     et en ajoutant une colonne pour le numéro de l'étape de transformation.
 
     Args:
         lineage_data (dict): Résultat de `track_fields_across_lineage`
         file_name (str): Nom du fichier Excel de sortie (par défaut "lineage_tracking.xlsx")
     """
     all_data = []
     previous_entry = None  # Variable pour stocker l'entrée précédente
     # Construire la liste initiale
     for field, entries in lineage_data.items():
         for entry in entries:
                         
             # Ajouter l'entrée à la liste all_data
             all_data.append({
                 "Tables utilisées": entry.get("Table(s) utilisées", ""),
                 "dwh_fields": entry.get("rdms_field", ""),
                 "Champ": entry.get("colonne", ""),
                 "Alias": entry.get("Alias", ""),
                 "Chemin du fichier HQL": entry["path"],
                 "Opérations arithmétiques": ", ".join(entry["Opérations arithmétiques"]),
                 "Formule SQL": entry["Formule SQL"]
             })
 
             # Mettre à jour l'entrée précédente
             previous_entry = entry
 
     # Convertir en DataFrame pour faciliter le traitement
     df = pd.DataFrame(all_data)
 
     # Grouper par dwh_fields
     grouped = df.groupby("dwh_fields")
 
     # Ajouter une colonne pour le numéro de l'étape
     df["Étape"] = 0  # Initialiser la colonne des étapes
 
     for dwh_field, group in grouped:
         # Trier les entrées par "Chemin du fichier HQL" (mettre les entrées sans chemin en dernier)
         group = group.sort_values(by="Chemin du fichier HQL", na_position="last")
 
         # Identifier les chemins uniques
         unique_paths = group["Chemin du fichier HQL"].dropna().unique()
         total_steps = len(unique_paths) + 1  # Ajouter 1 pour l'étape sans chemin
 
         # Attribuer les numéros d'étapes de manière décroissante
         step_mapping = {path: total_steps - i for i, path in enumerate(unique_paths, start=1)}
         step_mapping[None] = 1  # La dernière étape est pour les entrées sans chemin
 
         # Appliquer le mapping des étapes
         for index, row in group.iterrows():
             path = row["Chemin du fichier HQL"]
             df.loc[index, "Étape"] = step_mapping.get(path, 1)
 
     # Exporter vers Excel
     df = df.drop_duplicates()
        # Vérifier si l'alias de la ligne actuelle est présent dans "Formule SQL" de la ligne suivante
     filtered_dfs = []
    # Boucle sur les valeurs uniques de "rdms_field"
     for field in df["dwh_fields"].unique():
        sub_df = df[df["dwh_fields"] == field].copy()  # Filtrer par rdms_field
        # Décalage de "Formule SQL" d'une ligne vers le bas
        sub_df["Formule SQL Décalée"] = sub_df["Formule SQL"].shift(-1)
        sub_df["Alias Décalée"] = sub_df["Alias"].shift(-1)
        sub_df["Champ Décalé"] = sub_df["Champ"].shift(-1)
        # Vérifier si l'alias actuel est dans la "Formule SQL Décalée"
         # Vérification avant d'accéder à la première ligne
        first_alias = sub_df.iloc[0]["Alias"] if not sub_df.empty else None

        # Appliquer le masque
        mask = sub_df.apply(lambda row: 
                            isinstance(row["Alias"], str) and 
                            isinstance(row["Formule SQL Décalée"], str) and 
                            ((isinstance(row["Alias Décalée"], str)) and
                             isinstance(row["Champ Décalé"], str) and
                            (row["Alias"] == first_alias or 
                            row["Champ"] == first_alias) or
                            row["Champ Décalé"] == first_alias), 
                            axis=1)

        # Appliquer le filtre et ajouter au résultat
        filtered_dfs.append(sub_df[mask])
    # Affichage du résultat
     final_df = pd.concat(filtered_dfs, ignore_index=True)
     final_df=final_df.drop(columns=["Formule SQL Décalée", "Alias Décalée", "Champ Décalé"], inplace=True)
     print(final_df[final_df["dwh_fields"] == "BYTES_DEBITED"])

     #df.to_excel(file_name, index=False, engine="openpyxl")
    


"""

{

        "champ1": [{
            "chemin_du_fichier.hql": "query1.hql",
            "Opérations arithmétiques": ["+"],
            "Formule SQL": "SELECT champ1 FROM table_hive_1",
            "Table(s) utilisées": ["table_hive_1"]
        }, {
            "chemin_du_fichier.hql": "query1.hql",
            "Opérations arithmétiques": ["+"],
            "Formule SQL": "SELECT champ1 FROM table_hive_1",
            "Table(s) utilisées": ["table_hive_1"]
        },{
            "chemin_du_fichier.hql": "query1.hql",
            "Opérations arithmétiques": ["+"],
            "Formule SQL": "SELECT champ1 FROM table_hive_1",
            "Table(s) utilisées": ["table_hive_1"]
        }],
        "champ2": [{    },]
}

"""

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
