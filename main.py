from data_lineage.utils import extract_data_sources
from data_lineage.utils import process_conf_files
from data_lineage.utils import extract_tables_from_queries
from data_lineage.utils import get_dir_dependances_2
from data_lineage.utils import extract_hive_table_and_queries
from data_lineage.utils import map_rdms_file_hql_file
from data_lineage.utils import list_all_files
from data_lineage.utils import generate_excel_with_rdms_and_dependencies
from data_lineage.utils import display_table_dependencies_2
from data_lineage.utils import generate_excel_with_dependencies_3
from data_lineage.utils import parse_hql_file
from sqllineage.runner import LineageRunner
from data_lineage.utils import extract_hive_table_and_queries_paths
from data_lineage.format_json import create_scheduled_group_dict
from data_lineage.format_json import read_json
from data_lineage.format_json import update_dict_depedencies
from data_lineage.format_json import structure_dic_test
from data_lineage.format_json import structure_dic
import os

#a,b=extract_data_sources(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\IN_ZTE\EQUATION_PREPAYEE\compute_and_insert_into_spark_ft_prpd_eqt.hql")
# EXTRACTION TABLES PRINCIPALES ET TABLES DEPENDANTES
#a,b=extract_data_sources(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\MVAS\compute_and_insert_into_spark_ft_qos_smsc_special_number.hql")
#print("table principale",b)
#print("les tables",a)

if __name__ == "__main__":
    
    dic_dependencies_nifi={}
    #root_dir=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS"
    #scripts_dir= r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS"
    #scripts_dir=os.path.normpath(scripts_dir)
    #directory_conf = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
    #hql_path=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\IN_ZTE\prequery_spark_completude.hql"
    #paths_scripts=r'C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS'

    root_dir=r"C:\Users\dokie\Downloads\wetransfer_hdfs_2024-12-09_1245\HDFS\HDFS"
    #scripts_dir=r"C:\Users\dokie\Downloads\wetransfer_hdfs_2024-12-09_1245\HDFS\HDFS\PROD\SCRIPTS"
    #scripts_dir=os.path.normpath(scripts_dir)
    paths_scripts=r"C:\Users\dokie\Downloads\wetransfer_hdfs_2024-12-09_1245\HDFS\HDFS\PROD\SCRIPTS"
    directory_conf=r"C:\Users\dokie\Downloads\wetransfer_hdfs_2024-12-09_1245\HDFS\HDFS\PROD\CONF"
    file_scripts_paths=list_all_files(paths_scripts)
    #nifi_flow_file=r"C:\Users\YBQB7360\Documents\formated_PRODv2.0.json" #exemple de fichier nifi json
    #dic_nifi_flow_file = read_json(nifi_flow_file)
    dic_files_queries_paths = process_conf_files(directory_conf,root_dir)
    # dic table hive -> dependances
    dic_tables_dependances=get_dir_dependances_2(dic_files_queries_paths)
    #for i,value in dic_tables_dependances.items():
     #   print("i",i,"value",value)
    #table datawarehouse ->equivalent datalake
    dic_rdms_hive=extract_hive_table_and_queries(directory_conf)
    #display_table_dependencies_2(dic_tables_dependances,"AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY")
    dic_dependencies=generate_excel_with_rdms_and_dependencies(dic_rdms_hive,dic_tables_dependances, "dependencies_with_raw.xlsx")
    #print("equal","MON.FT_QOS_SMSC_SPECIAL_NUMBER" in dic_dependencies.keys())
    #print("unique_raw",unique_raw)
    server=None
    raw=None
    search_key = "componentType"
    search_value = "PROCESS_GROUP"
    #dic_process_group=create_scheduled_group_dict(dic_nifi_flow_file,search_key,search_value)
    #18313f9f-beec-18e7-84b5-451d52b6e6e0 
    #list_dic=structure_dic(dic_process_group,dic_dependencies)
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
    filter_list=tables = [
        "MON.FT_GLOBAL_ACTIVITY_DAILY",
        "DIM.DT_DATES",
        "DIM.DT_OFFER_PROFILES",
        "DIM.DT_DESTINATIONS",
        "DIM.DT_VAT_RATE",
        "MON.FT_A_GPRS_ACTIVITY",
        "VW_DT_CI_INFO",
        "FT_A_SUBSCRIBER_SUMMARY_B2B",
        "MON.FT_A_GPRS_LOCATION",
        "MON.SQ_FT_GOS_SVA",
        "MON.FT_A_VAS_REVENUE_DAILY",
        "DIM.DT_VAS_PARTNER",
        "MON.FT_A_SUBSCRIBER_SUMMARY",
        "MON.VW_DT_OFFER_PROFILES",
        "MON.FT_A_SUBSCRIPTION",
        "MON.FT_X_INTERCO_FINAL",
        "DT_SMS_APPLICATION_ACC_REF",
        "MON.FT_QOS_SMSC_SPECIAL_NUMBER",
        "DIM.DT_TIME_SLICES",
        "DIM.DT_USAGES",
        "MON.FT_GSM_TRAFFIC_REVENUE_DAILY",
        "MON.FT_A_DATA_TRANSFER",
        "MON.FT_GROSS_ADD_COMPETITIORS",
        "MON.FT_COMMERCIAL_SUBSCRIB_SUMMARY",
        "DT_DATES",
        "MON.VW_DT_DATES",
        "MON.FT_GLOBAL_ACTIVITY_DAILY_MKT"
    ]
    name_file="dependencies_with_raw_server.xlsx"
    #generate_excel_with_dependencies_3(dic_rdms_hive,dic_tables_dependances, list_dic, name_file,filter_list)

#map_dependencies_to_servers(dic_dependencies, list_dic)

