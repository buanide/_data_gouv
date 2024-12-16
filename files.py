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
from utils import generate_excel_with_rdms_and_dependencies_2


# Chemin vers le fichier HQL

if __name__ == "__main__":
    root_dir = "C:/Users/YBQB7360/Downloads/HDFS/HDFS"
    conf_dir = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
    scripts_dir= r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS"
    scripts_dir=os.path.normpath(scripts_dir)
    conf_dir=os.path.normpath(conf_dir) 
    #lecture des chemins des fichiers de configuration
    chains=list_all_files(conf_dir)
    #lecture des chemins des tous les scripts
    lists_paths_scripts=list_all_files(scripts_dir)
    #a=chains[0]
    #file_queries=extract_pre_exec_and_exec_queries_by_file(chains,root_dir)
    """
    dic=extract_table_names_from_load_conf_files(file_queries)
    print("dépendances des fichier load \n")

    for key, value in dic.items():
        print("la table",key)
        print("ses dependances",value)
    """

    dic_rdms_hive=extract_hive_table_and_queries(conf_dir)
    dic_tables_hive_paths=map_rdms_file_hql_file(dic_rdms_hive,lists_paths_scripts)
    dic_hive_depandances=extract_tables_from_hql(dic_tables_hive_paths)

    output_file = "dependencies_2.xlsx"

    for i,value in dic_hive_depandances.items():
        if i=="AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY":
            print("table hive",i)
            print("tables",value)
            for j in value:
                values = dic_hive_depandances.get(j, "Table Hive non trouvée")
                print("les dépendances de",j,"sont",values) 


    #generate_excel_with_rdms_and_dependencies(dic_rdms_hive,dic_hive_depandances,output_file)
    #dependency_chain = build_dependency_chain(dic_rdms_hive, dic_hive_depandances)
    # Affichage du dictionnaire de dépendances avant l'aplatissement
    """"
    print("Dictionnaire de dépendances avant l'aplatissement:")
    for rdms_table, data in dependency_chain.items():
        print(f"Table RDMS: {rdms_table}")
        print(f"Table Hive: {data['hive_table']}")
        print(f"Dépendances: {data['dependencies']}")
        print()
    """
    #flattened_dependencies = flatten_dependencies(dependency_chain,dic_hive_depandances)
    #print_dependencies(dependency_chain, dic_hive_depandances)
    

    #dic_rdm_hives=extract_hive_table_and_queries(conf_dir)
    #generate_excel_with_rdms_and_dependencies(dic_hive_rdms, dic_hive_depandances, "output_file_with_cycles.xlsx")


    #for i, value in dic_tables_hive_paths.items():
    #    print(i,"table","path",value)
    #dependency_map = extract_table_names_from_load_conf_files(file_queries)
    # Afficher les résultats

       
    #output_file = 'wanda_moi.xlsx'
    #generate_excel(dependency_map, output_file)