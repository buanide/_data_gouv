import sqlglot
from sqlglot import parse_one
from sqlglot import expressions as exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import find_all_in_scope
from sqlglot.optimizer.scope import build_scope
from data_lineage.fields import find_tables_in_select


hql_file_path=r'C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\BDI\FT_BDI\PP\insert_into_kyc_bdi_pp_step1.hql'
try:
    with open(hql_file_path, "r", encoding="utf-8") as f:
        hql_content = f.read()
except FileNotFoundError:
    print(f"Fichier introuvable: {hql_file_path}")
    
expression = sqlglot.parse_one(hql_content, read="hive")


try:
    expression_qualified = qualify(expression)
except sqlglot.errors.OptimizeError as e:
    print(f"Warning: {e}")  # Affiche un avertissement sans interrompre l'exécution
    expression_qualified = expression  

all_selects = list(expression_qualified.find_all(exp.Select))
for select_expr in all_selects:
        tables_in_select = find_tables_in_select(select_expr)
        # print("tablein select",tables_in_select)
        tables_str = ", ".join(tables_in_select) if tables_in_select else "Aucune table"

print(tables_str)

"""
# 1) Parser la requête
expr = parse_one(query, read="hive")

# 2) Trouver la ou les références de table (noeud de type Table)
table = expr.find(exp.Table)

if table is not None:
    #print("catalog :", table.args.get("catalog"))
    #print("db (schema) :", table.args.get("db"))     # pour "mon"
    #print("this (nom de la table) :", table.args.get("this"))  # pour "spark_ft_contract_snapshot"

    # Vérifier explicitement si c’est de la forme schema.table
    if table.db and table.this:
        print(f"La table est de la forme schema.table : {table.db}.{table.this}")
    else:
        print("La table n’est pas au format schema.table.")
"""
