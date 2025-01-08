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
    por une table en clé on a un eliste de champs en valeurs
    """
    expression = sqlglot.parse_one(hive_sql, read="hive")
    expression_qualified = qualify(expression)
    root = build_scope(expression_qualified)
    dic = {}
    
    for column in find_all_in_scope(root.expression, exp.Column):
        tables = extract_table_names(str(root.sources[column.table]))

        print(f"coloumn : {str(column).split('.')[1]} => source: {extract_table_names(str(root.sources[column.table]))}")
        print("")
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

hive="""

INSERT INTO AGG.FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
SELECT
C.PROFILE COMMERCIAL_OFFER_CODE
, B.GLOBAL_CODE TRANSACTION_TYPE
, IF(ACCT_RES_CODE='1','MAIN','PROMO') SUB_ACCOUNT
, '+' TRANSACTION_SIGN
, 'IN' SOURCE_PLATFORM
, 'IT_ZTE_ADJUSTMENT' SOURCE_DATA
, B.GLOBAL_CODE  SERVED_SERVICE
, B.GLOBAL_USAGE_CODE SERVICE_CODE
, 'DEST_ND' DESTINATION_CODE
, NULL SERVED_LOCATION
, NULL MEASUREMENT_UNIT
, SUM(1) RATED_COUNT
, SUM(1) RATED_VOLUME
, SUM(CHARGE/100) TAXED_AMOUNT
, SUM((1-0.1925) * CHARGE / 100 ) UNTAXED_AMOUNT
, CURRENT_TIMESTAMP INSERT_DATE
,'REVENUE' TRAFFIC_MEAN
, C.OPERATOR_CODE OPERATOR_CODE
, NULL LOCATION_CI
, CREATE_DATE TRANSACTION_DATE
FROM CDR.SPARK_IT_ZTE_ADJUSTMENT A
LEFT JOIN (SELECT USAGE_CODE, GLOBAL_CODE, GLOBAL_USAGE_CODE, FLUX_SOURCE FROM DIM.DT_ZTE_USAGE_TYPE ) B ON B.USAGE_CODE = A.CHANNEL_ID
LEFT JOIN (SELECT ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE 
               FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
               LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                          WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                          GROUP BY ACCESS_KEY) B 
                ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
               WHERE B.ACCESS_KEY IS NOT NULL                 
               GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE ) C
ON C.ACCESS_KEY = GET_NNP_MSISDN_9DIGITS(A.ACC_NBR)
WHERE CREATE_DATE = '###SLICE_VALUE###'  AND B.FLUX_SOURCE='ADJUSTMENT' AND CHANNEL_ID IN ('13','9','14','15','26','29','28','37', '109')
 AND CHARGE > 0
GROUP BY
 C.PROFILE
, B.GLOBAL_CODE
, IF(ACCT_RES_CODE='1','MAIN','PROMO')
, B.GLOBAL_CODE
, B.GLOBAL_USAGE_CODE
, C.OPERATOR_CODE
, CREATE_DATE

;
"""

def extract_table_details_with_partition_and_if_not_exists(file_path):
    """
    Extrait le nom de la table, les noms des champs, les informations de partition 
    et la présence de 'IF NOT EXISTS' d'une requête CREATE TABLE dans un fichier .hql.
    
    :param file_path: Chemin du fichier .hql
    :return: Un tuple contenant le nom de la table, une liste des champs, les informations de partition et si 'IF NOT EXISTS' est utilisé
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Adapter la regex pour prendre en compte 'IF NOT EXISTS', 'PARTITIONED BY' et autres options
        create_table_match = re.search(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\S+)\s*\((.*?)\)\s*(PARTITIONED\s+BY\s*\(.*?\))?\s*STORED\s+AS\s+\w+",
            content,
            re.DOTALL | re.IGNORECASE
        )
        
        if not create_table_match:
            # Vérifier sans la partie 'IF NOT EXISTS' (au cas où elle est absente)
            create_table_match = re.search(
                r"CREATE\s+TABLE\s+(\S+)\s*\((.*?)\)\s*(PARTITIONED\s+BY\s*\(.*?\))?\s*STORED\s+AS\s+\w+",
                content,
                re.DOTALL | re.IGNORECASE
            )
            if not create_table_match:
                print("Aucune requête CREATE TABLE partitionnée avec ou sans IF NOT EXISTS trouvée dans le fichier.")
                return None, [], None, False
        
        # Extraire le nom de la table
        table_name = create_table_match.group(1)
        
        # Extraire le contenu entre parenthèses pour les champs
        table_body = create_table_match.group(2)
        
        # Utiliser une regex pour capturer les noms des champs
        field_names = re.findall(r"\b(\w+)\b\s+\w+", table_body)
        
        # Extraire le champ de partition (s'il existe)
        partitioned_by = create_table_match.group(3)
        
        # Vérifier la présence de 'IF NOT EXISTS'
        if "IF NOT EXISTS" in content:
            if_not_exists = True
        else:
            if_not_exists = False
        
        return table_name, field_names, partitioned_by, if_not_exists
    
    except FileNotFoundError:
        print(f"Le fichier {file_path} est introuvable.")
        return None, [], None, False
    except Exception as e:
        print(f"Erreur : {e}")
        return None, [], None, False

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


def analyze_projection(projection: exp.Expression) -> dict:
    """
    Analyse une expression de projection (Alias(...) ou Colonne/fonction directe)
    et retourne :
      - Colonnes détectées
      - Fonctions d'agrégation
      - Opérations arithmétiques
      - Formule SQL
    """

    # 1) Colonnes
    columns_used = []
    for col in projection.find_all(exp.Column):
        # col.table, col.db, col.name
        # On reconstruit par exemple table.col
        table_part = col.table or "NO_TABLE"
        columns_used.append(f"{table_part}.{col.name}")

    # 2) Fonctions d'agrégation (SUM, COUNT, etc.)
    agg_funcs = []
    for func in projection.find_all(exp.AggFunc):
        agg_func_name = func.__class__.__name__.upper()  # "SUM", "COUNT", ...
        # Gérer le cas COUNT(*)
        if isinstance(func, exp.Count) and func.is_star:
            agg_func_name = "COUNT(*)"
        agg_funcs.append(agg_func_name)

    # 3) Opérations arithmétiques
    arithmetic_ops = []
    ARITHMETIC_NODES = (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod)
    for op in projection.find_all(ARITHMETIC_NODES):
        arithmetic_ops.append(type(op).__name__.upper())  # "SUB", "MUL", ...

    # 4) Formule SQL
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


def create_lineage_dic(hql_file_path: str) -> dict:
    """
    Lit une requête HQL depuis un fichier,
    parse et qualifie la requête, puis construit un dictionnaire
    dont la clé est le chemin du fichier et la valeur est un dict
    de la forme :

    {
      "ALIAS_NAME": {
        "Alias/Projection": ALIAS_NAME,
        "Colonnes détectées": [...],
        "Fonctions d'agg": [...],
        "Opérations arithmétiques": [...],
        "Formule SQL": ...,
        "Table(s) utilisées": ...
      },
      ...
    }
    """

    # Dictionnaire final, avec en clé le chemin, et en valeur un autre dict
    lineage_dict = {}

    # 1. Lecture du fichier HQL
    try:
        with open(hql_file_path, 'r', encoding='utf-8') as f:
            hql_content = f.read()
    except FileNotFoundError:
        print(f"Erreur : Le fichier '{hql_file_path}' n'a pas été trouvé.")
        return {}  # ou raise

    # 2. Parser la requête (dialecte Hive)
    expression = sqlglot.parse_one(hql_content, read="hive")
    if not expression:
        print(f"Erreur : Impossible de parser la requête dans '{hql_file_path}'.")
        return {}

    # 3. Qualifier la requête (associer les colonnes à leurs tables si possible)
    expression_qualified = qualify(expression)

    # 4. Trouver tous les SELECT
    all_selects = list(expression_qualified.find_all(exp.Select))

    # 5. Préparer le dictionnaire pour ce fichier
    #    => On veut un dict { "chemin fichier" : { alias: {...}, alias: {...} } }
    lineage_dict[hql_file_path] = {}

    # 6. Parcourir chaque SELECT
    for select_expr in all_selects:
        # Récupération des tables mentionnées
        tables_in_select = find_tables_in_select(select_expr)
        # On construit une chaîne pour "Table(s) utilisées"
        if tables_in_select:
            tables_str = ", ".join(tables_in_select)
        else:
            tables_str = "Aucune"

        # 7. Parcourir chaque projection
        for proj in select_expr.selects:
            # alias ou nom de projection
            if isinstance(proj, exp.Alias):
                alias_name = proj.alias or "NO_ALIAS"
                expr_to_analyze = proj.this
            else:
                alias_name = proj.alias_or_name or "NO_ALIAS"
                expr_to_analyze = proj

            # Extraire les infos sur la projection
            info = analyze_projection(expr_to_analyze)

            # Remplir la structure voulue
            lineage_dict[hql_file_path][alias_name] = {
                "Alias/Projection": alias_name,
                "Colonnes": info["columns_used"],
                "Fonctions agg": info["aggregations"],
                "Ops": info["arithmetic_ops"],
                "Formule SQL": info["formula_sql"],
                "Table(s) utilisées": tables_str
            }

    return lineage_dict


def print_lineage_dict(lineage_dict):
    """
    Affiche le dictionnaire de lineage de manière lisible.
    :param lineage_dict: dictionnaire retourné par create_lineage_dic(...)
    """

    for hql_path, aliases_info in lineage_dict.items():
        print(f"\n=== FICHIER HQL : {hql_path} ===")
        for alias_name, details in aliases_info.items():
            print(f"  - Alias/Projection : {alias_name}")
            print(f"    Colonnes détectées       : {details['Colonnes']}")
            print(f"    Fonctions d'agg          : {details['Fonctions agg']}")
            print(f"    Opérations arithmétiques : {details['Ops']}")
            print(f"    Formule SQL              : {details['Formule SQL']}")
            print(f"    Table(s) utilisées       : {details['Table(s) utilisées']}")
            print()  # ligne vide pour espacer


path=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\REPORT\GLOBAL_ACTIVITY\spark_compute_and_insert_adjustement_activity.hql"
result = create_lineage_dic(path)
print_lineage_dict(result)