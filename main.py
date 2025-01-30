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


    def structure_dic(dic_process_group: dict, dic_dependencies: dict):
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
                    staging_server=None
                    for process, elements in dic_process_group.items():
                        subdir = elements.get('subdir')
                        rep_raw = elements.get('rep_raw')
                        group_Identifier=elements.get('groupIdentifier')
                        flux_name = elements.get('flux_name')
                        # check if the raw path matches the process group raw path
                        if rep_raw and rep_raw == raw_base_path:
                            second_to_last = tab_raw[-2]
                            if subdir!=None:
                                # Check if subdir matches second_to_last
                                if second_to_last in subdir or second_to_last == subdir:
                                    staging_server = elements.get('staging')
                                    #list_servers.add(staging_server)
                                    flux_name = elements.get('flux_name')
                                    group_Identifier=elements.get('groupIdentifier')
                                    nb_processors=elements.get('nb_processors')
                                    nb_processors_disabeled=elements.get('nb_disabled')
                                    if staging_server!=None:
                                        
                                        dic[staging_server] = {"raw": raw_path,"flux_name":flux_name,"group_Identifier":group_Identifier,"nb_processors":nb_processors,"nb_processors_disabeled":nb_processors_disabeled}
                                    
        
                            else:
                                staging_server = elements.get('staging')
                                #list_servers.add(staging_server)
                                flux_name = elements.get('flux_name')
                                nb_processors=elements.get('nb_processors')
                                nb_processors_disabeled=elements.get('nb_disabled')
                                if staging_server!=None:  
                                    dic[staging_server] = {"raw": raw_path,"flux_name":flux_name,"group_Identifier":group_Identifier,"nb_processors":nb_processors,"nb_processors_disabeled":nb_processors_disabeled}
                        else:
                            # si on a un une raw coposé de deuéléments par exemple PROD/RAW dans le nifijson file
                            # on regarde dans le nifi file si l'avant dernier élément de la raw du renseigné 
                            # dans le projet est contenu dans les sous répertoirs de a raw du nifi file
                            if subdir!=None:
                                second_to_last = tab_raw[-2]
                                if second_to_last in subdir or second_to_last == subdir:
                                    #print("subdir",subdir,"second_to_last",second_to_last,"rep_raw",rep_raw)
                                    staging_server = elements.get('staging')
                                    list_servers.add(staging_server)
                                    flux_name = elements.get('flux_name')
                                    group_Identifier=elements.get('groupIdentifier')
                                    if staging_server!=None:
                                          for i,value in dic_process_group.items():
                                            group_Identifier_check=value.get('groupIdentifier')
                                            if group_Identifier_check==group_Identifier:
                                                nb_processors=value.get('nb_processors')
                                                nb_processors_disabeled=value.get('nb_disabled')

                                          dic[staging_server] = {"raw": raw_path,"flux_name":flux_name,"group_Identifier":group_Identifier,"nb_processors":nb_processors,"nb_processors_disabeled":nb_processors_disabeled}
                     
                    # Save the raw path and its associated servers
                
                
                #dic[table] = {"raw": raw_path, "servers": list(list_servers),"flux_name":flux_name}

        return dic
    

    def structure_dic_2(dic_process_group: dict, dic_dependencies: dict):
        """
        Constructs a dictionary where each server (staging) is a key, 
        and the associated raw path, number of processors, and flux name are stored in a nested dictionary.

        Args:
            dic_process_group (dict): Information on process groups with keys 'rep_raw', 'staging', 'subdir', 'nb_processors', etc.
            dic_dependencies (dict): Dependencies for the data warehouse tables.

        Returns:
            dict: A dictionary where each server (staging) is a key, and its associated raw path, nb_processors, and flux_name are stored.
        """
        dic = {}

        for value in dic_dependencies.values():
            dependencies = value.get('dependencies', [])
            if not dependencies:
                continue  # Skip if there are no dependencies

            last_dependency = dependencies[-1]  # Get the last dependency
            if last_dependency and last_dependency.startswith("/"):
                raw_path = last_dependency  # Detected raw directory

                # Extract the first 4 parts of the path to get the base raw directory
                tab_raw = raw_path.split("/")
                if len(tab_raw) > 3:
                    raw_base_path = "/".join(tab_raw[:4])  # Example: "/PROD/RAW/OM"

                    for process, elements in dic_process_group.items():
                        subdir = elements.get('subdir')
                        rep_raw = elements.get('rep_raw')
                        nb_processors = elements.get('nb_processors', 0)
                        staging_server = elements.get('staging')  # Use this as the key
                        flux_name = elements.get('flux_name', '')

                        if rep_raw and rep_raw == raw_base_path and staging_server:
                            second_to_last = tab_raw[-2]

                            # If subdir exists and matches the expected structure
                            if subdir and (second_to_last in subdir or second_to_last == subdir):
                                dic[staging_server] = {
                                    "raw": raw_path,
                                    "server":staging_server,
                                    "nb_processors": nb_processors,
                                    "flux_name": flux_name
                                }
                            elif not subdir:
                                dic[staging_server] = {
                                    "server":staging_server,
                                    "raw": raw_path,
                                    "nb_processors": nb_processors,
                                    "flux_name": flux_name
                                }

        return dic

   #18313f9f-beec-18e7-84b5-451d52b6e6e0 
    #dic=strucure_dic(dic_process_group,dic_dependencies)
    dic=structure_dic(dic_process_group,dic_dependencies)

    for i,value in dic.items():
        print("raw:",value.get('raw'),"server:",i,"flux_name:",value.get('flux_name'),"group identifier",value.get('group_Identifier'),"nb_processors",value.get('nb_processors'),"nb_processors_disabled",value.get('nb_processors_disabeled'))
    
        #if last.startswith("/"):
        #    server=last
         #   print("server",server)    
    #dic_rdms_paths_hive=extract_hive_table_and_queries_paths(directory_conf)
    #dic_rdms_fil_paths=map_rdms_file_hql_file(dic_rdms_hive,file_scripts_paths)
   

    