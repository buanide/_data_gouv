import sqlglot
from sqlglot import exp
from sqlglot import parse_one
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import find_all_in_scope
from sqlglot.optimizer.scope import build_scope
from sqlglot.lineage import lineage
import re
from sqlglot import exp



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
  

# =========================
# 1. Parser la requête
# =========================
    hive_sql = """
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


#print(expression)

# =========================
# 2. Fonction utilitaire
# =========================


query = """
    INSERT INTO  CDR.SPARK_IT_RATECHAN
SELECT
    Subs_id
     ,OLD_DEFAULT_PRICE_PLAN_ID
     ,NEW_DEFAULT_PRICE_PLAN_ID
     ,FROM_UNIXTIME(UNIX_TIMESTAMP(Update_date, 'dd/MM/yy hh:mm:ss')) Update_date
     ,CUID
     ,ORIGINAL_FILE_NAME
     ,ORIGINAL_FILE_SIZE
     ,ORIGINAL_FILE_LINE_COUNT
     ,CURRENT_TIMESTAMP() INSERT_DATE
     ,TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE

FROM CDR.TT_RATECHAN  C
         LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   CDR.SPARK_IT_RATECHAN) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL
    """
