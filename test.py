import sqlglot
import os
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
from data_lineage.fields import get_unique_tables_names_from_lineage_dict
from data_lineage.utils import extract_exec_queries
from data_lineage.utils import generate_dic_with_rdms_and_dependencies
from data_lineage.fields import get_hql_path_from_table_name
from data_lineage.utils import process_conf_files
from data_lineage.utils import get_dir_dependances_2
from data_lineage.fields import create_dict_tables_dependencies_and_path
from data_lineage.fields import create_dict_tables_dependencies_and_path_for_hive_tables
from data_lineage.fields import build_lineage
from data_lineage.fields import track_fields_across_lineage
from data_lineage.fields import track_fields_across_lineage_for_data_lake
import time
from data_lineage.utils import measure_execution_time
from data_lineage.fields import export_tracking_lineage_to_excel
from data_lineage.utils import display_table_dependencies_2
from data_lineage.format_json import read_json
from data_lineage.data_sources import data_sources_lineage
from data_lineage.fields import export_tracking_lineage_to_excel_2

# Démarrer le chronomètre
path=r"C:\\Users\\YBQB7360\\Downloads\\HDFS\\HDFS\\PROD\\SCRIPTS\\FT\\BDI\\FT_BDI_AMELIORE\\insert_into_spark_ft_bdi_ameliore.hql"
name_file=os.path.basename(path)
hdfs_dir = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS"
paths_scripts=r'C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS'
file_scripts_paths=list_all_files(paths_scripts)
create_table_dic=process_hql_files(file_scripts_paths)
#dic_table_fields=extract_lineage_fields(hql_content)
directory_conf = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
table_name='MON.FT_A_DATA_TRANSFER'
flow_file_path=r"C:\Users\YBQB7360\Documents\Data gouvernance\PRODv2.0\PRODv2.0.json"
#liste_table=list(dic_table_fields.keys())
#lineage_dic,_ = measure_execution_time(create_lineage_dic, path, create_table_dic)
#export_lineage_to_excel(lineage_dic, "lineage_"+name_file+".xlsx")
dict_fields_from_dwh=read_json(r"C:\Users\YBQB7360\Documents\Data gouvernance\_data_gouv\tables_mon_fields_description_dict.json")

dic_rdms_hive=extract_hive_table_and_queries(directory_conf)
dict_table_paths=map_rdms_file_hql_file(dic_rdms_hive,file_scripts_paths)
dic_files_queries_paths = process_conf_files(directory_conf, hdfs_dir)



# print("liste champs")
# a,b,c,d=extract_exec_queries(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF\ZEBRA\IT\load-it-zebra-master.conf")
# print("raw",c,"tt",d)
#list_table_from_hql=get_unique_tables_names_from_lineage_dict(lineage_dic)
#  dic table hive -> dependances
dic_tables_dependencies = get_dir_dependances_2(dic_files_queries_paths)
#display_table_dependencies_2(dic_tables_dependencies,"MON.SPARK_SMS_PARC")
dic_rdms_hive_dependencies=generate_dic_with_rdms_and_dependencies(dic_rdms_hive, dic_tables_dependencies)
# permet de ratacher à chaque source de données le ou les noms des hql qui l'alimente
dict_tables_dependencies_and_fields,_=measure_execution_time(create_dict_tables_dependencies_and_path,dict_table_paths,dic_rdms_hive_dependencies,create_table_dic,dic_files_queries_paths)

#data_sources_lineage(hdfs_dir,paths_scripts,directory_conf,flow_file_path,filter_list,"dependencies_with_raw_server_filtered.xlsx")  
#dict_tables_hive,_=measure_execution_time(create_dict_tables_dependencies_and_path_for_hive_tables,dict_table_paths,dic_tables_dependencies,create_table_dic)
"""
print("dict_tables_dependencies_and_fields")

for i,value in dict_tables_dependencies_and_fields.items():
    print("rdms_table",value.get('rdms_table',None))
    print("first_hive table",value.get('first_hive table',None))
    dependencies=value.get('dependencies',None)
    print(dependencies)
    break

print("dict_tables_hive")
for i,value in dict_tables_hive.items():
    print("i",i,"value",value)
    break
"""
#lineage_dic_for_one_chain_of_dependencies,t=measure_execution_time(build_lineage,dependencies,create_table_dic)

#lineage_fields_across_dependencies,t=measure_execution_time(track_fields_across_lineage_for_data_lake,table_name,dict_tables_dependencies_and_fields,create_table_dic,dict_tables_hive)
lineage_fields_across_dependencies,t=measure_execution_time(track_fields_across_lineage,table_name,dict_tables_dependencies_and_fields,create_table_dic,dict_fields_from_dwh)

#print("lineage_fields_across_dependencies",lineage_fields_across_dependencies)
#export_tracking_lineage_to_excel(lineage_fields_across_dependencies,"lineage_"+table_name+".xlsx")
export_tracking_lineage_to_excel_2(lineage_fields_across_dependencies,"lineage_"+table_name+".xlsx")
#dict_tables_hql_from_request_lineage=get_hql_path_from_table_name(dict_table_paths,list_table_from_hql)
#print(dict_tables_hql_from_request_lineage)
#nom="MON.FT_CONTRACT_SNAPSHOT"
#for i,value in dict_table_paths.items():
    #contrat=dict_table_paths.get(nom,None)
