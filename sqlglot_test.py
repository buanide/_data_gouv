import sqlglot
from sqlglot import parse_one
from sqlglot import expressions as exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import find_all_in_scope
from sqlglot.optimizer.scope import build_scope
from data_lineage.fields import find_tables_in_select

query = "SELECT * FROM ( Select a , b mon.spark_ft_contract_snapshot)"
expression = sqlglot.parse_one(query, read="hive")
expression_qualified = qualify(expression)
all_selects = list(expression_qualified.find_all(exp.Select))

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
