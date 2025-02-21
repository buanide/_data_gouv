import sqlglot
from sqlglot import parse_one
from sqlglot import expressions as exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import find_all_in_scope
from sqlglot.optimizer.scope import build_scope
from data_lineage.fields import find_tables_in_select
from data_lineage.fields import extract_lineage_fields
from sqlglot.errors import OptimizeError
from data_lineage.utils import list_all_files
from data_lineage.fields import process_hql_files
from data_lineage.fields import create_lineage_dic
from data_lineage.fields import remove_comments
from data_lineage.utils import measure_execution_time


hql_file_path=r'C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\CLIENT360\compute_and_insert_into_spark_ft_client_360.hql'
try:
    with open(hql_file_path, "r", encoding="utf-8") as file:
        content = file.read()
except FileNotFoundError:
    print(f"Le fichier {hql_file_path} est introuvable.")
    
content=remove_comments(content)
d=extract_lineage_fields(content)
hdfs_dir = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS"
paths_scripts=r'C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS'
file_scripts_paths=list_all_files(paths_scripts)
create_table_dic=process_hql_files(file_scripts_paths)
dic_table_fields=extract_lineage_fields(content)
directory_conf = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
#liste_table=list(dic_table_fields.keys())
lineage_dic,_ = measure_execution_time(create_lineage_dic, hql_file_path, create_table_dic)


#liste_table=list(dic_table_fields.keys())
#lineage_dic,_ = measure_execution_time(create_lineage_dic, hql_file_path, create_table_dic)


