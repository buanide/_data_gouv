import sqlglot
from sqlglot import exp
from sqlglot import parse_one
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import find_all_in_scope
from sqlglot.optimizer.scope import build_scope
import re



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





if __name__ == "__main__":
    hive_sql = """
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
SELECT
    COMMERCIAL_OFFER_CODE
     , TRANSACTION_TYPE
     , SUB_ACCOUNT
     , TRANSACTION_SIGN
     , SOURCE_PLATFORM
     , SOURCE_DATA
     , SERVED_SERVICE
     , SERVICE_CODE
     , DESTINATION_CODE
     , SERVED_LOCATION
     , MEASUREMENT_UNIT
     , RATED_COUNT
     , RATED_VOLUME
     , TAXED_AMOUNT
     , UNTAXED_AMOUNT
     , INSERT_DATE
     , TRAFFIC_MEAN
     , OPERATOR_CODE
     , LOCATION_CI
     , TRANSACTION_DATE
FROM(
        SELECT
            OFFER_PROFILE_CODE COMMERCIAL_OFFER_CODE
             ,(CASE SERVICE_CODE
                   WHEN 'VOI_VOX' THEN 'VOICE'
                   WHEN 'NVX_SMS' THEN 'SMS'
                   WHEN 'NVX_USS' THEN 'USSD'
                   ELSE SERVICE_CODE
            END) TRANSACTION_TYPE
             ,'MAIN' SUB_ACCOUNT
             ,'+' TRANSACTION_SIGN
             , SOURCE_PLATFORM
             ,'FT_GSM_TRAFFIC_REVENUE_DAILY'  SOURCE_DATA
             , SERVED_SERVICE
             , IF(SERVICE_CODE='NVX_USS','NVX_SMS',SERVICE_CODE) SERVICE_CODE
             , DESTINATION DESTINATION_CODE
             , OTHER_PARTY_ZONE SERVED_LOCATION
             , (CASE SERVICE_CODE
                    WHEN 'VOI_VOX' THEN 'DURATION'
                    WHEN 'NVX_SMS' THEN 'HIT'
                    WHEN 'NVX_USS' THEN 'HIT'
                    ELSE 'HIT' END ) MEASUREMENT_UNIT
             , SUM (TOTAL_COUNT) RATED_COUNT
             , SUM (CASE SERVICE_CODE
                        WHEN 'VOI_VOX' THEN DURATION
                        WHEN 'NVX_SMS' THEN TOTAL_COUNT
                        WHEN 'NVX_USS' THEN TOTAL_COUNT
                        ELSE TOTAL_COUNT END) RATED_VOLUME
             , SUM (MAIN_RATED_AMOUNT) TAXED_AMOUNT
             , SUM ((1-0.1925) * MAIN_RATED_AMOUNT) UNTAXED_AMOUNT
             , CURRENT_TIMESTAMP INSERT_DATE
             , 'REVENUE' TRAFFIC_MEAN
             , OPERATOR_CODE
             , TRANSACTION_DATE
             , LOCATION_CI
        FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
        WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND NVL(MAIN_RATED_AMOUNT,0) > 0
        GROUP BY
            OFFER_PROFILE_CODE
               ,(CASE SERVICE_CODE
                     WHEN 'VOI_VOX' THEN 'VOICE'
                     WHEN 'NVX_SMS' THEN 'SMS'
                     WHEN 'NVX_USS' THEN 'USSD'
                     ELSE SERVICE_CODE
            END)
               , SERVED_SERVICE
               , IF(SERVICE_CODE='NVX_USS','NVX_SMS',SERVICE_CODE)
               , DESTINATION
               , SOURCE_PLATFORM
               , OTHER_PARTY_ZONE
               , (CASE SERVICE_CODE
                      WHEN 'VOI_VOX' THEN 'DURATION'
                      WHEN 'NVX_SMS' THEN 'HIT'
                      WHEN 'NVX_USS' THEN 'HIT'
                      ELSE 'HIT' END )
               , OPERATOR_CODE
               , TRANSACTION_DATE
               , LOCATION_CI
        UNION
        SELECT
            OFFER_PROFILE_CODE COMMERCIAL_OFFER_CODE
             ,(CASE SERVICE_CODE
                   WHEN 'VOI_VOX' THEN 'VOICE'
                   WHEN 'NVX_SMS' THEN 'SMS'
                   WHEN 'NVX_USS' THEN 'USSD'
                   ELSE SERVICE_CODE
            END) TRANSACTION_TYPE
             ,'PROMO' SUB_ACCOUNT
             ,'+' TRANSACTION_SIGN
             , SOURCE_PLATFORM
             ,'FT_GSM_TRAFFIC_REVENUE_DAILY'  SOURCE_DATA
             , SERVED_SERVICE
             , IF(SERVICE_CODE='NVX_USS','NVX_SMS',SERVICE_CODE) SERVICE_CODE
             , DESTINATION DESTINATION_CODE
             , OTHER_PARTY_ZONE SERVED_LOCATION
             , (CASE SERVICE_CODE
                    WHEN 'VOI_VOX' THEN 'DURATION'
                    WHEN 'NVX_SMS' THEN 'HIT'
                    WHEN 'NVX_USS' THEN 'HIT'
                    ELSE 'HIT' END ) MEASUREMENT_UNIT
             , SUM (0) RATED_COUNT
             , SUM (0) RATED_VOLUME
             , SUM (PROMO_RATED_AMOUNT) TAXED_AMOUNT
             , SUM ((1-0.1925) * PROMO_RATED_AMOUNT) UNTAXED_AMOUNT
             , CURRENT_TIMESTAMP INSERT_DATE
             , 'REVENUE' TRAFFIC_MEAN
             , OPERATOR_CODE
             , TRANSACTION_DATE
             , LOCATION_CI
        FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
        WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND NVL(PROMO_RATED_AMOUNT,0) > 0
        GROUP BY
            OFFER_PROFILE_CODE
               ,(CASE SERVICE_CODE
                     WHEN 'VOI_VOX' THEN 'VOICE'
                     WHEN 'NVX_SMS' THEN 'SMS'
                     WHEN 'NVX_USS' THEN 'USSD'
                     ELSE SERVICE_CODE
            END)
               , SERVED_SERVICE
               , IF(SERVICE_CODE='NVX_USS','NVX_SMS',SERVICE_CODE)
               , DESTINATION
               , SOURCE_PLATFORM
               , OTHER_PARTY_ZONE
               , (CASE SERVICE_CODE
                      WHEN 'VOI_VOX' THEN 'DURATION'
                      WHEN 'NVX_SMS' THEN 'HIT'
                      WHEN 'NVX_USS' THEN 'HIT'
                      ELSE 'HIT' END )
               , OPERATOR_CODE
               , TRANSACTION_DATE
               , LOCATION_CI
    ) A
"""


def extract_lineage_fields(hive_sql):
    # 1. Parser
    expression = sqlglot.parse_one(hive_sql, read="hive")
    # 2. Qualifier
    expression_qualified = qualify(expression)
    # 3. Build scope
    scope = build_scope(expression_qualified)

    # scope.expression -> L'expression complète (SELECT ... FROM ...)

    # 4. Accéder à la liste des sélections
    select_items = scope.expression.select.expressions  # liste des colonnes/fonctions du SELECT
    lineage_info = []

    for item in select_items:
        # item = un élément du SELECT
        # ex: UPPER(a.name) AS name_up

        alias = item.alias_or_name  # ex: "name_up" ou "age_plus_ten" ou "city"
        operation, dependencies = analyze_expression(item)

        lineage_info.append({
            "alias": alias,
            "operation": operation,
            "dependencies": dependencies
        })

    return lineage_info


def analyze_expression(expr):
    """
    Renvoie (operation, dependencies)
    ex:
      - "UPPER", ["table_a.name"]
      - "ADD", ["table_b.age", "10"]  (10 est littéral, on peut le stocker tel quel)
      - "COLUMN", ["table_a.city"] si c’est juste un champ direct
    """
    # Si c'est juste une colonne
    if isinstance(expr, exp.Column):
        qualified_name = f"{expr.table}.{expr.name}" if expr.table else expr.name
        return ("COLUMN", [qualified_name])

    # Si c’est un alias explicite, on analyse l’expression enfant
    if isinstance(expr, exp.Alias):
        return analyze_expression(expr.this)

    # Si c’est un appel de fonction
    if isinstance(expr, exp.Function):
        # ex: UPPER(...)
        func_name = expr.name
        # on analyse les arguments
        deps = []
        for arg in expr.args.values():
            if isinstance(arg, list):
                # ex: function with multiple args
                for subarg in arg:
                    _, subdeps = analyze_expression(subarg)
                    deps.extend(subdeps)
            else:
                _, subdeps = analyze_expression(arg)
                deps.extend(subdeps)
        return (func_name.upper(), deps)

    # Si c’est un opérateur binaire (Add, Sub, Mul, Div, etc.)
    if isinstance(expr, exp.Binary):
        op = expr.op  # '+', '-', etc.
        _, left_deps = analyze_expression(expr.left)
        _, right_deps = analyze_expression(expr.right)
        return (op, left_deps + right_deps)

    # Si c’est une valeur littérale
    if isinstance(expr, exp.Literal):
        return ("LITERAL", [expr.name])

    # Autres cas : CAST, CASE WHEN, etc.
    # On traite de manière générale tous les "children" de expr
    # et on concatène leurs dépendances.
    operation_type = expr.__class__.__name__.upper()
    all_deps = []
    for child in expr.children:
        _, child_deps = analyze_expression(child)
        all_deps.extend(child_deps)

    return (operation_type, all_deps)




expression = sqlglot.parse_one(hive_sql, read="hive")
    # 2. Qualifier
expression_qualified = qualify(expression)
    # 3. Build scope
scope = build_scope(expression_qualified)

print(repr(expression_qualified))


#a=extract_lineage_fields(hive_sql)
#print(a)


"""
expression = sqlglot.parse_one(hive_sql, read="hive")
expression_qualified = qualify(expression)
root = build_scope(expression_qualified)
dic = {}

for column in find_all_in_scope(root.expression, exp.Column):
    tables = extract_table_names(str(root.sources[column.table]))
    print(f"coloumn : {str(column).split('.')[1]} => source: {str(root.sources[column.table])}")
    print("")
"""    