from utils import list_all_files
import re
import sqlparse
from collections import defaultdict
import sqlglot
from sqlglot import exp
from sqlglot import parse_one
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import find_all_in_scope
from sqlglot.optimizer.scope import build_scope
import pandas as pd

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

    table_names=set(table_names)
    
    return list(table_names)



def extract_lineage_fields(hive_sql):
    """
    Parse a Hive SQL query and extract the lineage fields.
    pour une table en clé on a un liste de champs en valeurs
    """
    expression = sqlglot.parse_one(hive_sql, read="hive")
    expression_qualified = qualify(expression)
    root = build_scope(expression_qualified)
    dic = {}
    
    for column in find_all_in_scope(root.expression, exp.Column):
        tables = extract_table_names(str(root.sources[column.table]))
        #print(f"coloumn : {str(column).split('.')[1]} => source: {extract_table_names(str(root.sources[column.table]))}")
        #print("")
        # Retirer les guillemets du champ
        a = str(column).split('.')[1].strip('"')
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



def extract_lineage_fields_2(hive_sql):
    """
    Analyse une requête Hive SQL pour extraire les alias, tables et champs associés.

    Args:
        hive_sql (str): La requête Hive SQL à analyser.

    Returns:
        dict: Dictionnaire où les clés sont les noms des tables et alias, 
              et les valeurs sont des listes des champs associés.


              key: mon.spark_ft_contract_snapshot valeur ['access_key', 'profile', 'operator_code']
key: dim.dt_zte_usage_type valeur ['global_code', 'global_usage_code', 'usage_code']
key: cdr.spark_it_zte_adjustment valeur ['acc_nbr', 'charge', 'create_date', 'channel_id', 'acct_res_code']
    """
    # Parser et qualifier la requête SQL
    expression = parse_one(hive_sql, read="hive")
    expression_qualified = qualify(expression)
    root = build_scope(expression_qualified)

    alias_to_table = {}
    table_columns = {}

    def process_expression(expression):
        """
        Parcourt récursivement une expression pour extraire les colonnes et leurs tables associées.

        Args:
            expression (sqlglot.Expression): L'expression à analyser.

        Returns:
            None: Met à jour directement les dictionnaires `alias_to_table` et `table_columns`.
        """
        if isinstance(expression, exp.Table):
            # Ajouter une table directe
            table_name = expression.name
            alias = expression.alias_or_name
            alias_to_table[alias] = table_name

        elif isinstance(expression, exp.Subquery):
            # Ajouter une sous-requête
            alias = expression.alias_or_name
            if alias:
                alias_to_table[alias] = f"(subquery: {expression.this.sql()})"

        elif isinstance(expression, exp.Column):
            # Ajouter une colonne associée à un alias ou une table
            alias = expression.table
            column = expression.alias_or_name
            if alias:
                if alias not in table_columns:
                    table_columns[alias] = set()
                table_columns[alias].add(column)

        # Parcourir récursivement les sous-expressions
        for child in expression.args.values():
            if isinstance(child, list):
                for sub_child in child:
                    if isinstance(sub_child, exp.Expression):
                        process_expression(sub_child)
            elif isinstance(child, exp.Expression):
                process_expression(child)

    # Traiter l'expression racine
    process_expression(root.expression)

    # Associer les alias aux tables principales (si possible)
    resolved_table_columns = {}
    for alias, table in alias_to_table.items():
        if table in table_columns:
            resolved_table_columns[table] = table_columns.get(alias, set())
        elif alias in table_columns:
            resolved_table_columns[alias] = table_columns[alias]
        else:
            resolved_table_columns[alias] = set()

    # Convertir les sets en listes pour la sortie finale
    for key in resolved_table_columns:
        resolved_table_columns[key] = list(resolved_table_columns[key])

    return resolved_table_columns

def extract_table_details_with_partition_and_if_not_exists(file_path):
    """
    Extrait le nom de la table, les noms des champs, les informations de partition
    et la présence de 'IF NOT EXISTS' d'une requête CREATE TABLE (ou CREATE EXTERNAL TABLE)
    dans un fichier .hql.

    Note : Dans cette version, tous les éléments (nom de table, noms de champs, clause de partition)
    sont renvoyés en majuscules. 'if_not_exists' reste un booléen.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # 1) CREATE TABLE IF NOT EXISTS ... PARTITIONED BY ...
        create_table_match = re.search(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\S+)\s*\((.*?)\)\s*(PARTITIONED\s+BY\s*\(.*?\))?\s*STORED\s+AS\s+\w+",
            content,
            re.DOTALL | re.IGNORECASE
        )
        
        if not create_table_match:
            # 2) CREATE TABLE ... PARTITIONED BY ... (sans IF NOT EXISTS)
            create_table_match = re.search(
                r"CREATE\s+TABLE\s+(\S+)\s*\((.*?)\)\s*(PARTITIONED\s+BY\s*\(.*?\))?\s*STORED\s+AS\s+\w+",
                content,
                re.DOTALL | re.IGNORECASE
            )
        
            # 3) CREATE EXTERNAL TABLE (IF NOT EXISTS) ... LOCATION ...
            if not create_table_match:
                create_table_match = re.search(
                    r"CREATE\s+EXTERNAL\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)\s*\((.*?)\)\s*(COMMENT\s+'.*?')?\s*ROW\s+FORMAT\s+DELIMITED\s+FIELDS\s+TERMINATED\s+BY\s+'.*?'\s*LOCATION\s+'(.*?)'",
                    content,
                    re.DOTALL | re.IGNORECASE
                )
                
                if not create_table_match:
                    print("Aucune requête CREATE (EXTERNAL) TABLE trouvée.")
                    return None, [], None, False
        
        # Déterminer si IF NOT EXISTS est présent dans le contenu (optionnel)
        if_not_exists = ("IF NOT EXISTS" in content.upper())
        
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
    


def resolve_column_alias(column_name: str,dic_path:dict,results: dict) -> str:
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
    split_col = column_name.split(".")
    if len(split_col) == 2:
        # alias = 'a', col_name = 'CHARGE' (ex.)
        _, col_name = split_col
    else:
        # Pas de point => la colonne est directe
        # (ex. col_name = 'CHARGE')
        #col_name=column_name
        pass

    # 2) Convertir en majuscules pour la comparaison
    
    for table_name, fields in dic_path.items():
        fields_upper = [f.upper() for f in fields]
        col_name = col_name.strip('`"').upper()
        #print("col name",col_name)
        if col_name in fields_upper:
            #print('operator_code true')
            return f"{table_name.upper()}.{col_name}"
        else:
            col_name = col_name.strip('`"').upper()
            for fp, table_info in results.items():
                if not isinstance(table_info, dict):
                    continue
                fields = table_info.get("fields", [])
                fields_upper = [f.upper() for f in fields]
                if col_name in fields_upper:
                    table_name = table_info.get("table_name", "").upper()  # on met le nom de table en maj
                    if table_name:
                        return f"{table_name}.{col_name}"   

    return column_name

def analyze_projection(projection: exp.Expression,hql_content:str,results: dict) -> dict:
    """
    Analyse une projection pour extraire :
      - columns_used : liste des colonnes (résolues si ambiguës) en minuscule
      - aggregations : liste des fonctions d'agrégation
      - arithmetic_ops : liste des opérations arithmétiques
      - formula_sql : la reconstitution de la projection en SQL
    """

    columns_used = []
    for col in projection.find_all(exp.Column):
        table_part = col.table or ""
        column_part = col.name
        raw_column_name = f"{table_part}.{column_part}" if table_part else column_part
        #print("raw_column_name",raw_column_name)
        dic_table_fields=extract_lineage_fields(hql_content)
        #print("raw_column_name")
        # Tenter de résoudre l'ambiguïté (ex. 'a.CHARGE' -> 'mon.spark_ft_contract_snapshot.charge')
        resolved = resolve_column_alias(raw_column_name,dic_table_fields, results)
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


def find_tables_in_select(select_expr: exp.Select) -> list:
    """
    Récupère toutes les tables mentionnées dans un SELECT donné
    (FROM, JOIN, etc.), sans doublons.
    Ex: ["mon.spark_ft_contract_snapshot", "dim.dt_zte_usage_type"]
    """
    tables = []
    for table_expr in select_expr.find_all(exp.Table):
        if table_expr.db:
            tables.append(f"{table_expr.db}.{table_expr.name}")
        else:
            tables.append(table_expr.name)
    return list(set(tables))


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

    try:
        with open(hql_file_path, 'r', encoding='utf-8') as f:
            hql_content = f.read()
    except FileNotFoundError:
        print(f"Fichier introuvable: {hql_file_path}")
        return {}

    expression = sqlglot.parse_one(hql_content, read="hive")
    if not expression:
        print(f"Impossible de parser le HQL dans: {hql_file_path}")
        return {}

    expression_qualified = qualify(expression)
    all_selects = list(expression_qualified.find_all(exp.Select))
    lineage_dict[hql_file_path] = {}
    for select_expr in all_selects:
        tables_in_select = find_tables_in_select(select_expr)
        tables_str = ", ".join(tables_in_select) if tables_in_select else "Aucune table"
        for proj in select_expr.selects:
            if isinstance(proj, exp.Alias):
                alias_name = proj.alias or "NO_ALIAS"
                expr_to_analyze = proj.this
            else:
                alias_name = proj.alias_or_name or "NO_ALIAS"
                expr_to_analyze = proj
            info = analyze_projection(expr_to_analyze,hql_content, results)
            lineage_dict[hql_file_path][alias_name] = {
                "Alias/Projection": alias_name,
                "Colonnes détectées": info["columns_used"],
                "agg": info["aggregations"],
                "Opérations arithmétiques": info["arithmetic_ops"],
                "Formule SQL": info["formula_sql"],
                "Table(s) utilisées": tables_str
            }

    return lineage_dict

def get_alias_table_in_dic(alias_name:str,dic_path:dict,results:dict,list_table:list)->str:
    """
    alias_name: alias ou colonnes dont on cherche la table
    results: dictionnaire décrivant toutes les tables
    dic_path: dictionnaire décrivant la requête hql 
    """
    for fp, table_info in results.items():
        if not isinstance(table_info, dict):
            continue
        fields = table_info.get("fields", [])
        fields_upper = [f.upper() for f in fields]
        if alias_name.upper() in fields_upper:
            #print("alias name in fields_upper",alias_name)
            table_name = table_info.get("table_name", "").upper()  # on met le nom de table en maj
            if table_name.lower() in list_table :
                return f"{table_name}.{alias_name.upper()}"   
            else:
                for table_name, fields in dic_path.items():
                    fields_upper = [f.upper() for f in fields]
                    #print("table name lower",table_name.lower())
                    #print("liset table",list_table)
                    
                   
                    
                    print("alias name", alias_name.upper(),"fields",fields_upper)

                    #print('table name lower',table_name.lower(),"list_table",list_table)
                    #print("")
                   
                    if alias_name.upper() in fields_upper and table_name.lower() in list_table:
                        print("alias:",alias_name)
                        print("table:",table_name)
                        return f"{table_name.upper()}.{alias_name.upper()}"



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
                "Colonnes détectées": ", ".join(details.get("Colonnes détectées", [])),
                "agg": ", ".join(details.get("agg", [])),
                "Opérations arithmétiques": ", ".join(details.get("Opérations arithmétiques", [])),
                "Formule SQL": details.get("Formule SQL", ""),
                "Tables utilisées": details.get("Table(s) utilisées", "")
            }
            excel_rows.append(row)

    # Créer un DataFrame pandas
    df = pd.DataFrame(excel_rows, columns=[
        "Nom du Fichier",
        "Alias/Projection",
        "Colonnes détectées",
        "agg",
        "Opérations arithmétiques",
        "Formule SQL",
        "Tables utilisées"
    ])
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
                table_name, fields_list, partitioned_by, if_not_exists = extract_table_details_with_partition_and_if_not_exists(file_path)
                results[file_path] = {
                    "table_name": table_name,
                    "fields": fields_list,
                    "partitioned_by": partitioned_by,
                    "if_not_exists": if_not_exists
                }
            except ValueError as e:
                print(f"Erreur lors du traitement du fichier {file_path}: {e}")
        else:
            print(f"Le fichier {file_path} n'est pas un fichier HQL ou ne contient pas de requête CREATE TABLE.")

    return results


def find_tables_in_select(select_expr: exp.Select) -> list:
    """
    Récupère toutes les tables mentionnées dans un SELECT donné
    (dans le FROM, les JOIN, etc.).
    Retourne une liste de noms de tables sous la forme SCHEMA.TABLE ou TABLE.
    """
    tables = []
    for table_expr in select_expr.find_all(exp.Table):
        # table_expr.db = schéma, table_expr.name = nom de la table
        # Ex : db=MON, name=FT_CONTRACT_SNAPSHOT
        if table_expr.db:
            tables.append(f"{table_expr.db}.{table_expr.name}")
        else:
            tables.append(table_expr.name)
    return list(set(tables))  # on retire les doublons








    
