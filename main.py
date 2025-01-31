from utils import extract_data_sources
from utils import process_conf_files
from utils import extract_tables_from_queries
from utils import get_dir_dependances_2
from utils import extract_hive_table_and_queries
from utils import map_rdms_file_hql_file
from utils import list_all_files
from utils import generate_excel_with_rdms_and_dependencies
from utils import display_table_dependencies_2
from utils import generate_excel_with_dependencies_3
from utils import parse_hql_file
from sqllineage.runner import LineageRunner
from utils import extract_hive_table_and_queries_paths
from format_json import create_scheduled_group_dict
from format_json import read_json
from format_json import update_dict_depedencies

from format_json import structure_dic
import pandas as pd

import os

#a,b=extract_data_sources(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\IN_ZTE\EQUATION_PREPAYEE\compute_and_insert_into_spark_ft_prpd_eqt.hql")
# EXTRACTION TABLES PRINCIPALES ET TABLES DEPENDANTES
#a,b=extract_data_sources(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\MVAS\compute_and_insert_into_spark_ft_qos_smsc_special_number.hql")
#print("table principale",b)
#print("les tables",a)

if __name__ == "__main__":
    
    dic_dependencies_nifi={}
    root_dir=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS"
    scripts_dir= r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS"
    scripts_dir=os.path.normpath(scripts_dir)
    directory_conf = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
    hql_path=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\IN_ZTE\prequery_spark_completude.hql"
    paths_scripts=r'C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS'
    file_scripts_paths=list_all_files(paths_scripts)
    nifi_flow_file=r"C:\Users\YBQB7360\Documents\fichier_formate.json" #exemple de fichier nifi json
    dic_nifi_flow_file = read_json(nifi_flow_file)

    dic_files_queries_paths = process_conf_files(directory_conf,root_dir)
    # dic table hive -> dependances
    dic_tables_dependances=get_dir_dependances_2(dic_files_queries_paths)
    #table datawarehouse ->equivalent datalake
    dic_rdms_hive=extract_hive_table_and_queries(directory_conf)
    #display_table_dependencies_2(dic_tables_dependances,"AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY")
    dic_dependencies=generate_excel_with_rdms_and_dependencies(dic_rdms_hive,dic_tables_dependances, "dependencies_with_raw.xlsx")
    #print("unique_raw",unique_raw)
    server=None
    raw=None
    search_key = "componentType"
    search_value = "PROCESS_GROUP"
    dic_process_group=create_scheduled_group_dict(dic_nifi_flow_file,search_key,search_value)
    #18313f9f-beec-18e7-84b5-451d52b6e6e0 
    list_dic=structure_dic(dic_process_group,dic_dependencies)

    #for i,value in dic_dependencies.items():
    #    print("table",i,"dependences",value)
    #updated_dict=update_dict_depedencies(dic,dic_dependencies)
    #for i,value in updated_dict.items():
        #print("key",i,"tables",value)

    #for i,value in dic.items():
     #   print("raw:",value.get('raw'),"server:",i,"flux_name:",value.get('flux_name'),"group identifier",value.get('group_Identifier'),"nb_processors",value.get('nb_processors'),"nb_processors_disabled",value.get('nb_processors_disabeled'))
    
        #if last.startswith("/"):
        #    server=last
         #   print("server",server)    
    #dic_rdms_paths_hive=extract_hive_table_and_queries_paths(directory_conf)
    #dic_rdms_fil_paths=map_rdms_file_hql_file(dic_rdms_hive,file_scripts_paths)
    generate_excel_with_dependencies_3(dic_rdms_hive,dic_tables_dependances, list_dic, "dependencies_with_raw_server.xlsx")


  
        

#map_dependencies_to_servers(dic_dependencies, list_dic)

