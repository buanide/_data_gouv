from utils import extract_data_sources
from utils import process_conf_files
from utils import extract_tables_from_queries

#a,b=extract_data_sources(r"C:\Users\dokie\Downloads\wetransfer_hdfs_2024-12-09_1245\HDFS\HDFS\PROD\SCRIPTS\IT\MVAS\insert_into_spark_it_smsc_mvas_a2p.hql")


# EXTRACTION TABLES PRINCIPALES ET TABLES DEPENDANTES
#a,b=extract_data_sources(r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS\FT\MVAS\compute_and_insert_into_spark_ft_qos_smsc_special_number.hql")
#print("table principale",b)
#print("les tables",a)
import os


directory_conf_path = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
root_dir=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS"
scripts_dir= r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS"
scripts_dir=os.path.normpath(scripts_dir)
result = process_conf_files(directory_conf_path, root_dir)


def create_dic_fil_queries(result):
    """
    Créer un dictionnaire contenant les chemins des fichiers en clé et les chemins des fichiers de requêtes en valeur
    
    """
    dic = {}
    for file_path, queries in result.items():
        print(f"Fichier: {file_path}")

        # Initialiser les listes avant les conditions
        list_tables_pre_exec = []
        list_tables_exec = []

        if queries['pre_exec']:  # Vérifier si la liste n'est pas vide
            for i in range(len(queries['pre_exec'])):
                list_tables_pre_exec.append(queries['pre_exec'][i])
        else:
            print("Aucune pre-exec query trouvée.")

        if queries['exec']:  # Vérifier si la liste n'est pas vide
            for i in range(len(queries['exec'])):
                list_tables_exec.append(queries['exec'][i])
        else:
            print("Aucune exec query trouvée.")

        dic[file_path] = {'pre_exec': list_tables_pre_exec, 'exec': list_tables_exec}

    return dic

dic_queries_paths=create_dic_fil_queries(result)

# Créer un dictionnaire contenant les fichiers et les tables associées

def create_dic_depednances(dic_queries_paths):
    """
    Créer un dictionnaire contenant la table principales et les tables dépendantes
    
    """
    dic={}
    
    for i, value in dic_queries_paths.items():
        print("file",i)
        list_exec = set()
        b=None
        if value['exec']:
            if len(value['exec']) > 1:
                for i in value['exec']:
                    a,b=extract_data_sources(i)
                    list_exec.add(b)
                list_exec=list(list_exec)
                _,b=extract_data_sources(list_exec[0])
            else:
                a,b=extract_data_sources(value['exec'][0])
        a=[]
        if value['pre_exec']:
            for i in value['pre_exec']:
                a,_=extract_data_sources(i)
                print("les tables",a)
        
        dic[b] = {'tables_dependantes': a}
        
   
dic_dep_tables=create_dic_depednances(dic_queries_paths)

for i, value in dic_dep_tables.items():
    print(i,"table principale",value['table_principale'],"tables dependantes",value['tables_dependantes'])
#for i, value in dic.items():
#    print(i,"table principale",value['table_principale'],"tables dependantes",value['tables_dependantes'])



            

