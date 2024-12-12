import re 
import os
from utils import extract_pre_exec_and_exec_queries_by_file
from utils import list_all_files
from utils import extract_table_names_from_load_conf_files
from utils import generate_excel


# Chemin vers le fichier HQL

if __name__ == "__main__":
    root_dir = "C:/Users/dokie/Downloads/wetransfer_hdfs_2024-12-09_1245/HDFS/HDFS"
    conf_dir = r"C:\Users\dokie\Downloads\wetransfer_hdfs_2024-12-09_1245\HDFS\HDFS\PROD\CONF"
    conf_dir=os.path.normpath(conf_dir) 
    chains=list_all_files(conf_dir)

    #a=chains[0]
  
    file_queries=extract_pre_exec_and_exec_queries_by_file(chains,root_dir)
    """
    dic=extract_table_names_from_load_conf_files(file_queries)
    print("dépendances des fichier load \n")

    for key, value in dic.items():
        print("la table",key)
        print("ses dependances",value)
    """
    
    dependency_map = extract_table_names_from_load_conf_files(file_queries)

    # Afficher les résultats
    output_file = 'dependencies.xlsx'
    generate_excel(dependency_map, output_file)