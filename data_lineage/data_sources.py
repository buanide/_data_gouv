from data_lineage.utils import process_conf_files
from data_lineage.utils import get_dir_dependances_2
from data_lineage.utils import extract_hive_table_and_queries
from data_lineage.utils import generate_dic_with_rdms_and_dependencies
from data_lineage.utils import generate_excel_with_filtered_dependencies_processors
from data_lineage.utils import generate_excel_with_all_dependencies_processors
from data_lineage.format_json import create_scheduled_group_dict
from data_lineage.format_json import read_json
from data_lineage.format_json import structure_dic
import os

def data_sources_lineage(root_dir:str,scripts_dir:str,directory_conf:str,nifi_flow_file,list_table_dwh:list,name_file,filtered:bool=False): 
    """

    Arg:
        root_dir(str): path of hdfs directory 
        scripts_dir(str): path of the scripts directory
        directory_conf(str): path of the directory conf
        nifi_flow_file(str):path of the nifi_flow_file
        filtered: bool: if True, the excel will be filtered by the list_table_dwh, if False, all dependencies will be included in the excel
        list_table_dwh(list): Data warehouse tables for which you want to find dependencies , ex: [
            "MON.FT_GLOBAL_ACTIVITY_DAILY",
            "MON.FT_A_GPRS_ACTIVITY",
            "MON.FT_A_GPRS_LOCATION",
            "MON.SQ_FT_GOS_SVA"]

        name_file(str): Name you want to give to the exel generated, "dependencies_with_raw_server_filtered.xlsx"
    """
    scripts_dir = os.path.normpath(scripts_dir)
    #exemple de fichier nifi json
    dic_nifi_flow_file = read_json(nifi_flow_file)
    dic_files_queries_paths = process_conf_files(directory_conf, root_dir)
    # dic table hive -> dependances
    
    dic_tables_dependances = get_dir_dependances_2(dic_files_queries_paths)
    # table datawarehouse ->equivalent datalake
    dic_rdms_hive = extract_hive_table_and_queries(directory_conf)
    # dependance dwh -> dependencies till raw of the project hdfs
    dic_dependencies = generate_dic_with_rdms_and_dependencies(dic_rdms_hive, dic_tables_dependances)
    search_key = "componentType"
    search_value = "PROCESS_GROUP"
    dic_process_group = create_scheduled_group_dict(dic_nifi_flow_file, search_key, search_value)
    #print("dic_process_group",list(dic_process_group.values())[0])
    list_dic = structure_dic(dic_process_group, dic_dependencies)

    if filtered==False:
        generate_excel_with_all_dependencies_processors(dic_rdms_hive, dic_tables_dependances, list_dic, name_file)
    else:
        generate_excel_with_filtered_dependencies_processors(dic_rdms_hive, dic_tables_dependances, list_dic, name_file, list_table_dwh)

    #for i in range(0,10):
     #   print(list_dic[0])
    #generate_excel_with_dependencies_processors(dic_rdms_hive,dic_tables_dependances, list_dic, name_file,list_table_dwh)

    

    