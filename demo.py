from data_lineage.data_sources import data_sources_lineage
from data_lineage.format_json import to_format
from data_lineage.utils import generate_excel_with_rdms_and_dependencies
from data_lineage.utils import process_conf_files
from data_lineage.utils import get_dir_dependances_2
from data_lineage.utils import extract_hive_table_and_queries
from data_lineage.clean import get_strange_conf

hdfs_dir = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS"
scripts_dir = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS"
directory_conf = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
nifi_flow_file = r"C:\Users\YBQB7360\Documents\PRODv2.0.json"
dic_nifi=to_format(nifi_flow_file)
name_file="dependencies_with_raw_server_filtered_try.xlsx"
filter_list = [
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
        "MON.FT_GLOBAL_ACTIVITY_DAILY_MKT",
    ]

#data_sources_lineage(hdfs_dir,scripts_dir,directory_conf,dic_nifi,filter_list,name_file)
dic_files_queries_paths = process_conf_files(directory_conf, hdfs_dir)
#get_strange_conf(dic_files_queries_paths)
    # dic table hive -> dependances
#dic_tables_dependances = get_dir_dependances_2(dic_files_queries_paths)
    # table datawarehouse ->equivalent datalake
#dic_rdms_hive = extract_hive_table_and_queries(directory_conf)
#generate_excel_with_rdms_and_dependencies(dic_rdms_hive, dic_tables_dependances, "dependencies_with_raw.xlsx")

