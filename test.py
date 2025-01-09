import sqlglot
from sqlglot import exp
from sqlglot import parse_one
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import find_all_in_scope
from sqlglot.optimizer.scope import build_scope
from sqlglot.lineage import lineage
import re
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from utils import list_all_files
from fields import process_hql_files
from fields import extract_lineage_fields
import pandas as pd



def find_projection_tables_in_insert(sql: str):
    """
    Parse an INSERT ... SELECT ... query, find the final SELECT's projections,
    and map each projection to the set of table aliases referenced.
    
    Returns a dict { projection_alias_or_name: set_of_table_aliases }
    """
    root = sqlglot.parse_one(sql)
    # If it's an INSERT, the 'this' part is typically the SELECT
    if isinstance(root, exp.Insert):
        select_node = root.this  # This should be an exp.Select
    else:
        # If not an insert, maybe it's already a SELECT. We'll handle that gracefully.
        select_node = root if isinstance(root, exp.Select) else root.find(exp.Select)
    if not isinstance(select_node, exp.Select):
        print("No SELECT found in the statement.")
        return {}
    projection_map = {}
    # Each item in select_node.expressions is a projection in the SELECT list
    for proj in select_node.expressions:
        alias_or_name = proj.alias_or_name  # e.g. "COMMERCIAL_OFFER_CODE"
        
        tables_in_this_proj = set()
        for col in proj.find_all(exp.Column):
            if col.table:
                tables_in_this_proj.add(col.table)
        
        projection_map[alias_or_name] = tables_in_this_proj

    return projection_map


def resolve_column_alias(column_name: str,dic_path:dict,results: dict) -> str:
    """
    Tente de résoudre une colonne potentiellement ambiguë (ex: 'a.CHARGE')
    en cherchant dans  le dictionnaire decrivant toutes les creates tables (results[file_path]) laquelle
    possède ce champ dans 'fields'. 
    Si il ne parvient pas à retrouver le nom dans le dictionnaire décrivant toutes les tables ils essaient de retrouver le champs
    dans un dictionnaire de la forme 'dic_path' : {'mon.spark_ft_contract_snapshot': ['operator_code', 'access_key', 'profile'], 
    'dim.dt_zte_usage_type': ['usage_code', 'global_code', 'global_usage_code'], 
    'cdr.spark_it_zte_adjustment': ['channel_id', 'acct_res_code', 'acc_nbr', 'charge', 'create_date']}
    Tous les noms sont comparés en minuscule.

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
        pass

    # 2) Convertir en majuscules pour la comparaison
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
        else:
            for table_name, fields in dic_path.items():
                fields_upper = [f.upper() for f in fields]
                if col_name in fields_upper:
                    return f"{table_name.upper()}.{col_name}"

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



path=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\REPORT\GLOBAL_ACTIVITY\spark_compute_and_insert_adjustement_activity.hql"

#lineage_res = create_lineage_dic("example.hql", results)
#print_lineage_dict(lineage_res)
paths_scripts=r'C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS'
file_scripts_paths=list_all_files(paths_scripts)
create_table_dic=process_hql_files(file_scripts_paths)
lineage_dic = create_lineage_dic(path,create_table_dic)
#print_lineage_dict(lineage_dic)  
export_lineage_to_excel(lineage_dic, "lineage_results.xlsx")      