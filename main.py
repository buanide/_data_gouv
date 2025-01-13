from utils import extract_data_sources
from utils import process_conf_files
from utils import extract_tables_from_queries
from utils import get_dir_dependances
from utils import extract_hive_table_and_queries
from utils import map_rdms_file_hql_file
from utils import list_all_files
from utils import generate_excel_with_rdms_and_dependencies
from utils import display_table_dependencies
from utils import parse_hql_file
from sqllineage.runner import LineageRunner
from simple_ddl_parser import parse_from_file
from simple_ddl_parser import DDLParser
from utils import extract_hive_table_and_queries_paths

import os

#a,b=extract_data_sources(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\IN_ZTE\EQUATION_PREPAYEE\compute_and_insert_into_spark_ft_prpd_eqt.hql")
# EXTRACTION TABLES PRINCIPALES ET TABLES DEPENDANTES
#a,b=extract_data_sources(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\MVAS\compute_and_insert_into_spark_ft_qos_smsc_special_number.hql")
#print("table principale",b)
#print("les tables",a)

if __name__ == "__main__":
    
    root_dir=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS"
    scripts_dir= r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS"
    scripts_dir=os.path.normpath(scripts_dir)
    directory_conf = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
    hql_path=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\IN_ZTE\prequery_spark_completude.hql"
    paths_scripts=r'C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS'
    file_scripts_paths=list_all_files(paths_scripts)

    #result = DDLParser(ddl).run(output_mode="hql")
    #print(result)
    #dic_files_queries_paths = process_conf_files(directory_conf,root_dir)
    # dic table hive -> dependances
    #dic_tables_dependances=get_dir_dependances(dic_files_queries_paths)
    #table datawarehouse ->equivalent datalake
    dic_rdms_hive=extract_hive_table_and_queries(directory_conf)
    #generation des dependances de la table TANGO_CDR.IT_OMNY_USER_REGISTRATION_V2
    #display_table_dependencies(dic,"TANGO_CDR.IT_OMNY_USER_REGISTRATION_V2")
    #generate_excel_with_rdms_and_dependencies(dic_rdms_hive,dic_tables_dependances, "stephane.xlsx")
    #dic_rdms_paths_hive=extract_hive_table_and_queries_paths(directory_conf)
    dic_rdms_fil_paths=map_rdms_file_hql_file(dic_rdms_hive,file_scripts_paths)

    for i, value in dic_rdms_fil_paths.items():
        print("i:",i,"value:",value)

    