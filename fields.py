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


