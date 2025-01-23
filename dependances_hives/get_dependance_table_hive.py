from utils import extract_data_sources
from utils import process_conf_files
from utils import extract_tables_from_queries
from utils import get_dir_dependances_2
from utils import list_all_files
from utils import display_table_dependencies_2
from utils import extract_hive_table_and_queries_paths
import os


if __name__ == "__main__":
    root_dir=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS"
    scripts_dir= r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS"
    scripts_dir=os.path.normpath(scripts_dir)
    directory_conf = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
    paths_scripts=r'C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS'
    file_scripts_paths=list_all_files(paths_scripts)
    dic_files_queries_paths = process_conf_files(directory_conf,root_dir)
    dic_tables_dependances=get_dir_dependances_2(dic_files_queries_paths)
    display_table_dependencies_2(dic_tables_dependances,"AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY")
   
    

    