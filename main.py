from utils import extract_data_sources
from utils import process_conf_files
from utils import extract_tables_from_queries
from utils import get_dir_dependances_2
from utils import extract_hive_table_and_queries
from utils import map_rdms_file_hql_file
from utils import list_all_files
from utils import generate_excel_with_rdms_and_dependencies
from utils import display_table_dependencies_2
from utils import parse_hql_file
from sqllineage.runner import LineageRunner
from simple_ddl_parser import parse_from_file
from simple_ddl_parser import DDLParser
from utils import extract_hive_table_and_queries_paths
from format_json import create_scheduled_group_dict
from format_json import read_json
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
    nifi_flow_file=r"C:\Users\YBQB7360\Documents\fichier_formate.json" 
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


    def find_process_groups_with_rep_raw(path, strucure_dic, dic_process_group):
        if path not in strucure_dic:
            print(f"Path {path} not found in strucure_dic")
            return []

        rep_raw_value = strucure_dic[path].get("rep_raw")
        if not rep_raw_value:
            print(f"No rep_raw value found for path {path}")
            return []

        matching_groups = []
        for group_name, group_info in dic_process_group.items():
            if group_info.get("rep_raw") == rep_raw_value:
                matching_groups.append(group_name)

        return matching_groups
    

    dic_process_group=create_scheduled_group_dict(dic_nifi_flow_file,search_key,search_value)

    def strucure_dic(dic_process_group:dict,dic_dependencies:dict):
        """
        Construit un dictionnaire contenant un le chemin du répertoire raw et son serveur associé, pour chaque table détectée, 
        le répertoire raw (dossier source) et l'éventuel serveur associé.
        dic_process_group: dictionnaire contenant les informations sur les process groupes
        dic_dependencies: dictionnaire contenant les informations sur les dépendances des tables du datawarehouse
        """
        dic={}

        # pour chaque process group, on récupère le répertoire raw et le serveur associé
        for process,elements in dic_process_group.items():
            rep_raw=elements.get('rep_raw')
            server=None
            #print("num:",i,"process:",value)
            if rep_raw!=None:
                for i,value in dic_dependencies.items():
                    list_server=set()
                    dependencies=value.get('dependencies')
                    last=dependencies[-1]
                    # on récupère la dernière dépendance qui être un répertoire raw
                    if last!=None and last.startswith("/"):
                        raw=last
                        if raw=="/PROD/RAW/OM/TRANSACTIONS/Transactions*":
                            tab_raw = raw.split("/")
                            if len(tab_raw) > 3:
                                second_to_last = tab_raw[-2]
                                #print("second_to_last",second_to_last)
                                # on reconstruit le chemin du répertoire raw d'une table CDR jusqu'à son troisième élément pour le comparer avec le répertoire raw du process group
                                path_to_search = "/".join(tab_raw[:4])
                                #print("path_to_search",path_to_search)
                                #print("subdir",subdir)
                                if path_to_search == rep_raw:
                                    subdir=elements.get('subdir')
                                    if elements.get('staging')!=None:
                                        #print("server",elements.get('staging'),'subdir',subdir)
                                        if subdir!=None:
                                            print("subdir here",subdir,"second_to_last",second_to_last,"path_to_search",path_to_search)
                                            # on vérifie que le répertoire qu'un sous répertoire de la raw du CDR se trouve dans la liste des sous répertoirs des process group 
                                            if second_to_last in subdir:
                                                print("subdir",subdir,"second_to_last",second_to_last)
                                                #server=elements.get('staging')      
                                            elif second_to_last==subdir:
                                                print("subdir equal",subdir,"second_to_last",second_to_last)
                                                #server=elements.get('staging')
                                        else:
                                            print("subdir vide")
                                            #print("pas de subdir pour:",path_to_search,"server:",elements.get('staging'))
                                            #server=elements.get('staging')
                                            #list_server.append(server)
                                    #else:
                                       #print("path_to_search:",path_to_search,"server Inconnu","groupIdentifier:",elements.get('groupIdentifier'))
                            else :
                                print("autre cas")
                        dic[i]={"raw":raw,"server":list_server}
        return dic
    

    import logging



    def structure_dic_2(dic_process_group: dict, dic_dependencies: dict):
        """
        Construit un dictionnaire contenant le chemin du répertoire raw et tous les serveurs associés pour chaque table détectée.

        Args:
            dic_process_group (dict): Informations sur les process groupes avec les clés 'rep_raw', 'staging', 'subdir'.
            dic_dependencies (dict): Informations sur les dépendances des tables du datawarehouse.

        Returns:
            dict: Dictionnaire où chaque table est associée à son répertoire raw et ses serveurs détectés.
        """
        dic = {}

        for table, value in dic_dependencies.items():
            dependencies = value.get('dependencies', [])
            if not dependencies:
                continue  # Skip empty dependencies

            last_dependency = dependencies[-1]  # Get the last dependency
            if last_dependency and last_dependency.startswith("/"):
                raw_path = last_dependency  # Detected raw directory

                # Extract the first 4 parts of the path to get the base raw directory
                tab_raw = raw_path.split("/")
                if len(tab_raw) > 3:
                    raw_base_path = "/".join(tab_raw[:4])  # Example: "/PROD/RAW/OM"

                    # Collect all servers for this raw path
                    list_servers = set()

                    for process, elements in dic_process_group.items():
                        rep_raw = elements.get('rep_raw')
                        subdir = elements.get('subdir')
                        staging_server = elements.get('staging')

                        # Check if the raw path matches
                        if rep_raw and rep_raw == raw_base_path:
                            second_to_last = tab_raw[-2]  # Extract second-to-last directory

                            if subdir:
                                # Check if subdir matches second_to_last
                                if second_to_last in subdir or second_to_last == subdir:
                                    list_servers.add(staging_server)
                            else:
                                list_servers.add(staging_server)  # If no subdir, still add

                    # Save the raw path and its associated servers
                    dic[table] = {"raw": raw_path, "servers": list(list_servers)}

        return dic

    #dic=strucure_dic(dic_process_group,dic_dependencies)
    dic=structure_dic_2(dic_process_group,dic_dependencies)

    for i,value in dic.items():
        print("raw:",value.get('raw'),"servers:",value.get('servers'))
    
        #if last.startswith("/"):
        #    server=last
         #   print("server",server)    
    #dic_rdms_paths_hive=extract_hive_table_and_queries_paths(directory_conf)
    #dic_rdms_fil_paths=map_rdms_file_hql_file(dic_rdms_hive,file_scripts_paths)
   

    