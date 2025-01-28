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
    server=None
    raw=None
    search_key = "componentType"
    search_value = "PROCESS_GROUP"
    dic_process_group=create_scheduled_group_dict(dic_nifi_flow_file,search_key,search_value)

    def strucure_dic(dic_process_group:dict,dic_dependencies:dict):
        """
        Construit un dictionnaire décrivant, pour chaque table détectée, 
        le répertoire raw (dossier source) et l'éventuel serveur associé.
        """
        dic={}
        for process,elements in dic_process_group.items():
            rep_raw=elements.get('rep_raw')
            server=None
            #print("num:",i,"process:",value)
            if rep_raw!=None:
                for i,value in dic_dependencies.items():
                    dependencies=value.get('dependencies')
                    last=dependencies[-1]
                    if last!=None and last.startswith("/"):
                        raw=last
                        tab_raw = raw.split("/")
                        if len(tab_raw) > 3:
                            second_to_last = tab_raw[-2]
                            # les raw qui ont plus de 3 éléments dans leurs chemin de répertoire
                            path_to_search = "/".join(tab_raw[:3])
                            subdir=elements.get('subdir')
                            if path_to_search == rep_raw:
                                if elements.get('staging')!=None:
                                    if subdir!= None:
                                        if second_to_last in subdir:
                                            #print("table",i,"server",elements.get('staging'),"subdir",subdir)
                                            if ";" in subdir:
                                                subdir=subdir.split(";")
                                                for i in subdir:
                                                    if second_to_last == i:
                                                        print("table",i,"server",elements.get('staging'))
                                            server=elements.get('staging')
                                    else:
                                        #print("table",i,"server",elements.get('staging'))
                                        server=elements.get('staging')
                                else:
                                    print("table",i,"server Inconnu")
                        else :
                            print("autre cas")
                        dic[i]={"raw":raw,"server":server}
        return dic

    

    dic=strucure_dic(dic_process_group,dic_dependencies)
        #if last.startswith("/"):
        #    server=last
         #   print("server",server)    
    #dic_rdms_paths_hive=extract_hive_table_and_queries_paths(directory_conf)
    #dic_rdms_fil_paths=map_rdms_file_hql_file(dic_rdms_hive,file_scripts_paths)
   

    