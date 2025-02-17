import sqlglot
from sqlglot import exp
from sqlglot import parse_one
from sqlglot.optimizer.scope import find_all_in_scope
from sqlglot.optimizer.scope import build_scope
from sqlglot.lineage import lineage
import re
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from data_lineage.utils import list_all_files
from data_lineage.fields import process_hql_files
from data_lineage.fields import extract_lineage_fields
from data_lineage.fields import export_lineage_to_excel
from data_lineage.fields import create_lineage_dic
from data_lineage.fields import print_lineage_dict
from data_lineage.utils import map_rdms_file_hql_file
from data_lineage.utils import extract_hive_table_and_queries
import os
from data_lineage.fields import get_unique_tables_names_from_lineage_dict
from data_lineage.utils import extract_exec_queries
from data_lineage.utils import generate_dic_with_rdms_and_dependencies
from data_lineage.fields import get_hql_path_from_table_name
from data_lineage.utils import process_conf_files
from data_lineage.utils import get_dir_dependances_2
hql_content = """
INSERT INTO AGG.FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
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
   FROM MON.FT_CONTRACT_SNAPSHOT
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
   FROM MON.FT_CONTRACT_SNAPSHOT
   WHERE EVENT_DATE = '###SLICE_VALUE###' AND DEACTIVATION_DATE = '###SLICE_VALUE###'  
    AND PROMO_CREDIT > 0
   GROUP BY  
    DEACTIVATION_DATE
    ,UPPER(PROFILE)
    , OPERATOR_CODE
) A;
"""

path=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\REPORT\GLOBAL_ACTIVITY\compute_and_insert_contract_snapshot_activity.hql"
name_file=os.path.basename(path)
hdfs_dir = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS"
paths_scripts=r'C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS'
file_scripts_paths=list_all_files(paths_scripts)
create_table_dic=process_hql_files(file_scripts_paths)
dic_table_fields=extract_lineage_fields(hql_content)
directory_conf = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
#liste_table=list(dic_table_fields.keys())
lineage_dic = create_lineage_dic(path,create_table_dic)
# print_lineage_dict(lineage_dic)
#export_lineage_to_excel(lineage_dic, "lineage_results_pardon_"+name_file+".xlsx")
dic_rdms_hive=extract_hive_table_and_queries(directory_conf)
dict_table_paths=map_rdms_file_hql_file(dic_rdms_hive,file_scripts_paths)
dic_files_queries_paths = process_conf_files(directory_conf, hdfs_dir)
# print("liste champs")
# a,b,c,d=extract_exec_queries(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF\ZEBRA\IT\load-it-zebra-master.conf")
# print("raw",c,"tt",d)

list_table_from_hql=get_unique_tables_names_from_lineage_dict(lineage_dic)
#  dic table hive -> dependances
dic_tables_dependencies = get_dir_dependances_2(dic_files_queries_paths)
dic_rdms_hive_depedencies=generate_dic_with_rdms_and_dependencies(dic_rdms_hive, dic_tables_dependencies)



print("dic rdms hive")
for i,value in dic_rdms_hive_depedencies.items():
    print("i",i,"value",value)
    break

print("dic hive dependencies")
for i,value in dic_tables_dependencies.items():
    print("i",i,"value",value)
    break
#dict_tables_hql_from_request_lineage=get_hql_path_from_table_name(dict_table_paths,list_table_from_hql)
#print(dict_tables_hql_from_request_lineage)





#nom="MON.FT_CONTRACT_SNAPSHOT"
#for i,value in dict_table_paths.items():
    #contrat=dict_table_paths.get(nom,None)
    
   

#print(contrat)
    

#ohrr = r"ange"
#parts = ohrr.split(";")
# parts = ["", "PROD", "RAW", "IN_ZTE", "PRICE_PLAN_EXTRACT", "Data_*"]
#print(parts)
# if len(parts) > 3:
# Rejoindre tous les éléments sauf le dernier ("Data_*")
# Cela donnera: "/PROD/RAW/IN_ZTE/PRICE_PLAN_EXTRACT"
#    path = "/".join(parts[:-1])
#    print(path)
# else:
#    path=ohrr
