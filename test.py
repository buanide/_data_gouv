import sqlglot
from sqlglot import exp
from sqlglot import parse_one
from sqlglot.optimizer.scope import find_all_in_scope
from sqlglot.optimizer.scope import build_scope
from sqlglot.lineage import lineage
import re
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from utils import list_all_files
from fields import process_hql_files
from fields import extract_lineage_fields
from fields import export_lineage_to_excel
from fields import create_lineage_dic
from fields import print_lineage_dict
from fields import get_alias_table_in_dic
from utils import map_rdms_file_hql_file
from utils import extract_hive_table_and_queries
import os
from utils import extract_exec_queries

hql_content="""
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
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
FROM (SELECT * FROM CDR.SPARK_IT_ZTE_ADJUSTMENT WHERE CREATE_DATE = '###SLICE_VALUE###'  AND CHANNEL_ID IN ('13','9','14','15','26','29','28','37', '109','119', '120')
  AND CHARGE > 0 ) A 
         LEFT JOIN (SELECT USAGE_CODE, GLOBAL_CODE, GLOBAL_USAGE_CODE, FLUX_SOURCE FROM DIM.DT_ZTE_USAGE_TYPE  WHERE FLUX_SOURCE='ADJUSTMENT' ) B ON B.USAGE_CODE = A.CHANNEL_ID
         LEFT JOIN ( 
                    select ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE 
                    from MON.SPARK_FT_CONTRACT_SNAPSHOT where EVENT_DATE = '###SLICE_VALUE###'
                    group by ACCESS_KEY, PROFILE

                    ) C
                   ON C.ACCESS_KEY = GET_NNP_MSISDN_9DIGITS(A.ACC_NBR)
GROUP BY
    C.PROFILE
       , B.GLOBAL_CODE
       , IF(ACCT_RES_CODE='1','MAIN','PROMO')
       , B.GLOBAL_CODE
       , B.GLOBAL_USAGE_CODE
       , C.OPERATOR_CODE
       , CREATE_DATE
"""

path=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\REPORT\GLOBAL_ACTIVITY\compute_and_insert_contract_snapshot_activity.hql"
name_file=os.path.basename(path)
paths_scripts=r'C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS'
file_scripts_paths=list_all_files(paths_scripts)
create_table_dic=process_hql_files(file_scripts_paths)
dic_table_fields=extract_lineage_fields(hql_content)

#directory_conf = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
#liste_table=list(dic_table_fields.keys())


#lineage_dic = create_lineage_dic(path,create_table_dic)
#extract_lineage_fields(hql)
#print_lineage_dict(lineage_dic)  
#export_lineage_to_excel(lineage_dic, "lineage_results_2_"+name_file+".xlsx")
#dic_rdms_hive=extract_hive_table_and_queries(directory_conf)     
#dict_table_paths=map_rdms_file_hql_file(dic_rdms_hive,file_scripts_paths)
#for i,value in dict_table_paths.items():
#    print("i:",i,"value:",value)


#a,b,c,d=extract_exec_queries(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF\ZEBRA\IT\load-it-zebra-master.conf")

#print("raw",c,"tt",d)


