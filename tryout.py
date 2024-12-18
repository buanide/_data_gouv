from utils import extract_data_sources

#a,b=extract_data_sources(r"C:\Users\dokie\Downloads\wetransfer_hdfs_2024-12-09_1245\HDFS\HDFS\PROD\SCRIPTS\IT\MVAS\insert_into_spark_it_smsc_mvas_a2p.hql")


a,b=extract_data_sources(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\REPORT\GLOBAL_ACTIVITY\spark_compute_and_insert_adjustement_activity.hql")
print("table principale",b)
print("les tables",a)