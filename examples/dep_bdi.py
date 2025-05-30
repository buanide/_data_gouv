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
import time
from data_lineage.utils import measure_execution_time
from data_lineage.fields import export_tracking_lineage_to_excel_2
from data_lineage.utils import display_table_dependencies_2
from data_lineage.format_json import read_json
from data_lineage.fields import export_tracking_lineage_to_excel
from data_lineage.utils import extract_data_sources
from data_lineage.utils import display_table_dependencies_for_datalake_tables
import json
from data_lineage.utils import to_format

# Démarrer le chronomètre


hdfs_dir = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS"
paths_scripts=r'C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS'
file_scripts_paths=list_all_files(paths_scripts)
create_table_dic=process_hql_files(file_scripts_paths)
#dic_table_fields=extract_lineage_fields(hql_content)
directory_conf = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
#print(tables)
dic_files_queries_paths = process_conf_files(directory_conf, hdfs_dir)
#  dic table hive -> dependances
dic_tables_dependencies = get_dir_dependances_2(dic_files_queries_paths)

# 
#with open("donnees.json", "w", encoding="utf-8") as fichier:
#    json.dump(dic_tables_dependencies, fichier, indent=4, ensure_ascii=False)

#data: dictionnaisse des tables hive et leurs dépendances
data = json.load('C:/Users/YBQB7360/Documents/Data gouvernance/_data_gouv/donnees.json')


#display_table_dependencies_for_datalake_tables(dic_tables_dependencies)

