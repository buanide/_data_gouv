import sqlglot
from sqlglot import exp
from sqlglot import parse_one





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
     , NULL LOCATION_CI
     , TRANSACTION_DATE
FROM(
        SELECT
            DEACTIVATION_DATE TRANSACTION_DATE
             ,UPPER(PROFILE) COMMERCIAL_OFFER_CODE
             ,'DEACTIVATED_ACCOUNT_BALANCE' TRANSACTION_TYPE
             ,'MAIN' SUB_ACCOUNT
             ,'-' TRANSACTION_SIGN
             , 'IN' SOURCE_PLATFORM
             ,'FT_CONTRACT_SNAPSHOT'  SOURCE_DATA
             , 'IN_ACCOUNT' SERVED_SERVICE
             , 'NVX_BALANCE' SERVICE_CODE
             , 'DEST_ND' DESTINATION_CODE
             , NULL SERVED_LOCATION
             ,'HIT' MEASUREMENT_UNIT
             , SUM (1) RATED_COUNT
             , SUM (1) RATED_VOLUME
             , SUM (MAIN_CREDIT) TAXED_AMOUNT
             , SUM ((1-0.1925) * MAIN_CREDIT) UNTAXED_AMOUNT
             , CURRENT_TIMESTAMP INSERT_DATE
             ,'REVENUE' TRAFFIC_MEAN
             , OPERATOR_CODE OPERATOR_CODE
        FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND DEACTIVATION_DATE = '###SLICE_VALUE###'
          AND MAIN_CREDIT > 0
        GROUP BY
            DEACTIVATION_DATE
               ,UPPER(PROFILE)
               , OPERATOR_CODE
        UNION
        SELECT
            DEACTIVATION_DATE TRANSACTION_DATE
             ,UPPER(PROFILE) COMMERCIAL_OFFER_CODE
             ,'DEACTIVATED_ACCOUNT_BALANCE' TRANSACTION_TYPE
             ,'PROMO' SUB_ACCOUNT
             ,'-' TRANSACTION_SIGN
             , 'IN' SOURCE_PLATFORM
             ,'FT_CONTRACT_SNAPSHOT'  SOURCE_DATA
             , 'IN_ACCOUNT' SERVED_SERVICE
             , 'NVX_BALANCE' SERVICE_CODE
             , 'DEST_ND' DESTINATION_CODE
             , NULL SERVED_LOCATION
             ,'HIT' MEASUREMENT_UNIT
             , SUM (1) RATED_COUNT
             , SUM (1) RATED_VOLUME
             , SUM (PROMO_CREDIT) TAXED_AMOUNT
             , SUM ((1-0.1925) * PROMO_CREDIT) UNTAXED_AMOUNT
             , CURRENT_TIMESTAMP INSERT_DATE
             ,'REVENUE' TRAFFIC_MEAN
             , OPERATOR_CODE OPERATOR_CODE
        FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND DEACTIVATION_DATE = '###SLICE_VALUE###'
          AND PROMO_CREDIT > 0
        GROUP BY
            DEACTIVATION_DATE
               ,UPPER(PROFILE)
               , OPERATOR_CODE
    ) A

"""

    expression = sqlglot.parse_one(hive_sql, read="hive")

    # 2) Convert (write) the expression to "mysql" dialect
    mysql_sql = expression.sql(dialect="mysql")

    print("Original (Hive)  =>\n", hive_sql)
    print("Transformed (MySQL) =>\n", mysql_sql)