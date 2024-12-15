import re 
import os
from utils import extract_pre_exec_and_exec_queries_by_file
from utils import list_all_files
from utils import extract_table_names_from_load_conf_files
from utils import generate_excel
from utils import extract_hive_table_and_queries
from utils import map_rdms_file_hql_file
from utils import extract_tables_from_hql
from utils import generate_excel_with_rdms_and_dependencies


# Chemin vers le fichier HQL

if __name__ == "__main__":
    root_dir = "C:/Users/dokie/Downloads/wetransfer_hdfs_2024-12-09_1245/HDFS/HDFS/PROD/SCRIPTS"
    conf_dir = r"C:\Users\dokie\Downloads\wetransfer_hdfs_2024-12-09_1245\HDFS\HDFS\PROD\CONF"
    scripts_dir= r"C:\Users\dokie\Downloads\wetransfer_hdfs_2024-12-09_1245\HDFS\HDFS\PROD\SCRIPTS"
    scripts_dir=os.path.normpath(scripts_dir)
    conf_dir=os.path.normpath(conf_dir) 
    #lecture des chemins des fichiers de configuration
    chains=list_all_files(conf_dir)
    #lecture des chemins des tous les scripts
    lists_paths_scripts=list_all_files(scripts_dir)
    #a=chains[0]
    file_queries=extract_pre_exec_and_exec_queries_by_file(chains,root_dir)
    """
    dic=extract_table_names_from_load_conf_files(file_queries)
    print("dépendances des fichier load \n")

    for key, value in dic.items():
        print("la table",key)
        print("ses dependances",value)
    """

    dic_hive_rdms=extract_hive_table_and_queries(conf_dir)
    dic_tables_hive_paths=map_rdms_file_hql_file(dic_hive_rdms,lists_paths_scripts)
    dic_hive_depandances=extract_tables_from_hql(dic_tables_hive_paths)
    dic_rdm_hives=extract_hive_table_and_queries(conf_dir)
    generate_excel_with_rdms_and_dependencies(dic_rdm_hives, dic_hive_depandances, "output_file_with_cycles.xlsx")


    #for i, value in dic_tables_hive_paths.items():
    #    print(i,"table","path",value)
    #dependency_map = extract_table_names_from_load_conf_files(file_queries)
    # Afficher les résultats

       
    #output_file = 'wanda_moi.xlsx'
    #generate_excel(dependency_map, output_file)