import sqlglot
import os
from sqlglot import exp
from data_lineage.utils import list_all_files
from data_lineage.fields import process_hql_files
from data_lineage.utils import map_rdms_file_hql_file
from data_lineage.utils import extract_hive_table_and_queries
from data_lineage.utils import generate_dic_with_rdms_and_dependencies
from data_lineage.utils import process_conf_files
from data_lineage.utils import get_dir_dependances_2
from data_lineage.fields import create_dict_tables_dependencies_and_path
from data_lineage.fields import create_dict_tables_dependencies_and_path_for_hive_tables
from data_lineage.fields import build_lineage
from data_lineage.fields import track_fields_across_lineage
from data_lineage.fields import track_fields_across_lineage_for_data_lake
from data_lineage.utils import display_table_dependencies_for_datalake_tables
import time
from data_lineage.utils import measure_execution_time
from data_lineage.fields import export_tracking_lineage_to_excel_2
from data_lineage.utils import display_table_dependencies_2
from data_lineage.format_json import read_json
from data_lineage.fields import export_tracking_lineage_to_excel
from data_lineage.data_sources import data_sources_lineage

# Démarrer le chronomètre
hdfs_dir = r"C:\Users\ybqb7360\Downloads\hdfs-prod-bk-master" #ex :C:\Users\dokie\Downloads\hdfs-prod-bk-master\hdfs-prod-bk-master
paths_scripts=r'"C:\Users\ybqb7360\Downloads\hdfs-prod-bk-master\PROD\SCRIPTS'
file_scripts_paths=list_all_files(paths_scripts)
create_table_dic=process_hql_files(file_scripts_paths)
directory_conf = r"C:\Users\ybqb7360\Downloads\hdfs-prod-bk-master\PROD\CONF"
table_name='MON.FT_GLOBAL_ACTIVITY_DAILY'
flow_file_path=r"C:\Users\ybqb7360\OneDrive - orange.com\Bureau\toolbox\data lineage\_data_gouv\PRODv2.0.json"
# load the datawarehouse dictionary tables->fields
dict_fields_from_dwh=read_json(r"C:\Users\ybqb7360\OneDrive - orange.com\Bureau\toolbox\data lineage\_data_gouv\tables_mon_fields_description_dict.json") 
dic_rdms_hive=extract_hive_table_and_queries(directory_conf)
dict_table_paths=map_rdms_file_hql_file(dic_rdms_hive,file_scripts_paths)
dic_files_queries_paths = process_conf_files(directory_conf, hdfs_dir)
#  dic table hive -> dependances
dic_tables_dependencies = get_dir_dependances_2(dic_files_queries_paths)
# génération des dependances des tables du datalake sans info sur les serveurs (use case)
display_table_dependencies_for_datalake_tables(dic_tables_dependencies)
#display_table_dependencies_2(dic_tables_dependencies,"MON.SPARK_SMS_PARC")
#dic_rdms_hive_dependencies=generate_dic_with_rdms_and_dependencies(dic_rdms_hive, dic_tables_dependencies)
# permet de ratacher à chaque source de données le ou les noms des hql qui l'alimente
#dict_tables_dependencies_and_fields,_=measure_execution_time(create_dict_tables_dependencies_and_path,dict_table_paths,dic_rdms_hive_dependencies,create_table_dic,dic_files_queries_paths)

"""
for i,value in dict_tables_dependencies_and_fields.items():
    rdm=value.get('rdms_table',None)
    if rdm==table_name:
        #print("i",i,"value",value)
        #print("rdms_table",value.get('rdms_table',None))
        #print("first_hive table",value.get('first_hive table',None))
        dependencies=value.get('dependencies',None)
        lineage=build_lineage(dependencies,create_table_dic)
        print("lineage",lineage)
        break
"""
filter_list=[]
data_sources_lineage(hdfs_dir,paths_scripts,directory_conf,flow_file_path,filter_list,"dependencies_with_raw_server.xlsx")  
#dict_tables_hive,_=measure_execution_time(create_dict_tables_dependencies_and_path_for_hive_tables,dict_table_paths,dic_tables_dependencies,create_table_dic)
#print("dict_tables_dependencies_and_fields")
#lineage_dic_for_one_chain_of_dependencies,t=measure_execution_time(build_lineage,dependencies,create_table_dic)
#lineage_fields_across_dependencies,t=measure_execution_time(track_fields_across_lineage_for_data_lake,table_name,dict_tables_dependencies_and_fields,create_table_dic,dict_tables_hive)
#lineage_fields_across_dependencies,t=measure_execution_time(track_fields_across_lineage,table_name,dict_tables_dependencies_and_fields,create_table_dic,dict_fields_from_dwh)
#print("lineage_fields_across_dependencies",lineage_fields_across_dependencies)
#export_tracking_lineage_to_excel_2(lineage_fields_across_dependencies,"lineage_sorted"+table_name+".xlsx")