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
from data_lineage.fields import create_dict_tables_dependencies_and_path
from data_lineage.fields import get_unique_tables_names_from_lineage_dict
from data_lineage.utils import extract_hive_table_and_queries
from data_lineage.utils import map_rdms_file_hql_file
from data_lineage.utils import process_conf_files
from data_lineage.utils import get_dir_dependances_2
from data_lineage.utils import generate_dic_with_rdms_and_dependencies
from data_lineage.fields import build_lineage
from data_lineage.fields import track_fields_across_lineage
from data_lineage.fields import track_fields_across_lineage_for_data_lake
from data_lineage.fields import export_tracking_lineage_to_excel



hdfs_dir = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS"
paths_scripts=r'C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS'
file_scripts_paths=list_all_files(paths_scripts)
create_table_dic=process_hql_files(file_scripts_paths)
#dic_table_fields=extract_lineage_fields(content)
directory_conf = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
#liste_table=list(dic_table_fields.keys())
dic_rdms_hive=extract_hive_table_and_queries(directory_conf)
dict_table_paths=map_rdms_file_hql_file(dic_rdms_hive,file_scripts_paths)
dic_files_queries_paths = process_conf_files(directory_conf, hdfs_dir)
# print("liste champs")
# a,b,c,d=extract_exec_queries(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF\ZEBRA\IT\load-it-zebra-master.conf")
# print("raw",c,"tt",d)
#list_table_from_hql=get_unique_tables_names_from_lineage_dict(lineage_dic)
#  dic table hive -> dependances
dic_tables_dependencies = get_dir_dependances_2(dic_files_queries_paths)
dic_rdms_hive_dependencies=generate_dic_with_rdms_and_dependencies(dic_rdms_hive, dic_tables_dependencies)
dict_tables_dependencies_and_fields,_=measure_execution_time(create_dict_tables_dependencies_and_path,dict_table_paths,dic_rdms_hive_dependencies,create_table_dic)


#for i,value in dict_tables_dependencies_and_fields.items():
    #rdms_table=value.get('rdms_table',None)
    #if rdms_table=='MON.FT_A_DATA_TRANSFER':
        #print("rdms_table",rdms_table)
    #print("first_hive table",value.get('first_hive table',None))
        #dependencies=value.get('dependencies',None)
        #print("dependencies",dependencies)
        #break
    #print(dependencies)

#lineage_dic_for_one_chain_of_dependencies,t=measure_execution_time(build_lineage,dependencies,create_table_dic)
table_name='MON.SPARK_SMS_PARC'
#for i,value in lineage_dic_for_one_chain_of_dependencies.items():
#    print("path",i)
 #   print("dic",value)
  #  break
#print("dict_tables_dependencies_and_fields")
#print("dependencies",dependencies)
#rdms_table_name='MON.FT_GLOBAL_ACTIVITY_DAILY'
lineage_fields_across_dependencies,t=measure_execution_time(track_fields_across_lineage_for_data_lake,table_name,dict_tables_dependencies_and_fields,create_table_dic)
#export_tracking_lineage_to_excel(lineage_fields_across_dependencies, table_name+"lineage_fields_across_dependencies.xlsx")
#print("lineage_fields_across_dependencies",lineage_fields_across_dependencies)
#lineage_dic,_ = measure_execution_time(create_lineage_dic, hql_file_path, create_table_dic)
#liste_table=list(dic_table_fields.keys())
#lineage_dic,_ = measure_execution_time(create_lineage_dic, hql_file_path, create_table_dic)