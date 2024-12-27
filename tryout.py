from utils import extract_data_sources
from utils import process_conf_files
from utils import extract_tables_from_queries
import os

a,b=extract_data_sources(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\IN_ZTE\EQUATION_PREPAYEE\compute_and_insert_into_spark_ft_prpd_eqt.hql")


# EXTRACTION TABLES PRINCIPALES ET TABLES DEPENDANTES
#a,b=extract_data_sources(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\MVAS\compute_and_insert_into_spark_ft_qos_smsc_special_number.hql")
print("table principale",b)
print("les tables",a)



directory_conf_path = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
root_dir=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS"
scripts_dir= r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS"
scripts_dir=os.path.normpath(scripts_dir)




directory = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"


def lister_fichiers_repertoire(repertoire):
    fichiers_absolus = []
    for dossier, sous_dossiers, fichiers in os.walk(repertoire):
        for fichier in fichiers:
            chemin_absolu = os.path.join(dossier, fichier)
            try:
                # Vérifie si le fichier a un encodage valide
                chemin_absolu.encode('utf-8')
                fichiers_absolus.append(chemin_absolu)
            except UnicodeEncodeError:
                print(f"Fichier avec nom invalide : {chemin_absolu}")
    return fichiers_absolus

# Exemple d'utilisation
repertoire = 'C:/Users/YBQB7360/Downloads/HDFS/HDFS/PROD/CONF'
fichiers = lister_fichiers_repertoire(repertoire)


# Créer un dictionnaire contenant les fichiers et les tables associées



#dic_tables,table_sans_exec=create_dic_tables(dic_queries_paths)





"""
for i, value in dic_tables.items():
    print("file",i)
    print("table principale:",value['table_principale'])
    print("tables dependantes",value['tables_dépendantes'])
"""


    


            

