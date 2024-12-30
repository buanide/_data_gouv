from utils import extract_data_sources
from utils import process_conf_files
from utils import extract_tables_from_queries
from utils import get_dir_dependances
from utils import extract_hive_table_and_queries
from utils import map_rdms_file_hql_file
from utils import list_all_files
from utils import generate_excel_with_table_dependencies
from utils import display_table_dependencies
from sqllineage import lineage
import os

#a,b=extract_data_sources(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\IN_ZTE\EQUATION_PREPAYEE\compute_and_insert_into_spark_ft_prpd_eqt.hql")
# EXTRACTION TABLES PRINCIPALES ET TABLES DEPENDANTES
#a,b=extract_data_sources(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\MVAS\compute_and_insert_into_spark_ft_qos_smsc_special_number.hql")
#print("table principale",b)
#print("les tables",a)

root_dir=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS"
scripts_dir= r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS"
scripts_dir=os.path.normpath(scripts_dir)
lists_paths_scripts=list_all_files(scripts_dir)
directory_conf = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
sql = """
CREATE TABLE output_table AS
SELECT a.id, b.name
FROM input_table_a a
JOIN input_table_b b ON a.id = b.id;
"""

print(lineage(sql))





#-----------------------------------------------------------------------------
#dic_files_queries_paths = process_conf_files(directory_conf,root_dir)
#dic_tables_dependances=get_dir_dependances(dic_files_queries_paths)
#dic_rdms_hive=extract_hive_table_and_queries(directory_conf)
#for i,value in dic_rdms_hive.items():
    #   print(value)
#dic_tables_hive_paths=map_rdms_file_hql_file(dic_rdms_hive,lists_paths_scripts)
#dic_hive_depandances=extract_tables_from_hql(dic_tables_hive_paths)
#dic=get_dir_dependances(dic_files_queries_paths)
#display_table_dependencies(dic,"TANGO_CDR.IT_OMNY_USER_REGISTRATION_V2")

#generate_excel_with_table_dependencies(dic_tables_dependances, "output_file_with_cyclesv5.xlsx")


    


            

