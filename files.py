import re 
import os
from utils import extract_pre_exec_and_exec_queries_by_file
from utils import list_all_files
from utils import extract_table_names_from_load_conf_files
from utils import generate_excel
from utils import extract_hive_table_and_queries
from utils import map_rdms_file_hql_file
from utils import extract_tables_from_hql
from utils import generate_excel_with_rdms_and_dependencies
from utils import generate_excel_with_rdms_and_dependencies_2
from utils import extract_data_sources


# Chemin vers le fichier HQL

if __name__ == "__main__":
    root_dir = "C:/Users/YBQB7360/Downloads/HDFS/HDFS"
    conf_dir = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF"
    scripts_dir= r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS"
    scripts_dir=os.path.normpath(scripts_dir)
    conf_dir=os.path.normpath(conf_dir) 
    #lecture des chemins des fichiers de configuration
    chains=list_all_files(conf_dir)
    
    #lecture des chemins des tous les scripts
    lists_paths_scripts=list_all_files(scripts_dir)
    #a=chains[0]
    #file_queries=extract_pre_exec_and_exec_queries_by_file(chains,root_dir)
    """
    dic=extract_table_names_from_load_conf_files(file_queries)
    print("dépendances des fichier load \n")

    for key, value in dic.items():
        print("la table",key)
        print("ses dependances",value)
    """

    dic_rdms_hive=extract_hive_table_and_queries(conf_dir)
    #for i,value in dic_rdms_hive.items():
     #   print(value)
    dic_tables_hive_paths=map_rdms_file_hql_file(dic_rdms_hive,lists_paths_scripts)
    dic_hive_depandances=extract_tables_from_hql(dic_tables_hive_paths)

    #output_file = "dependencies_2.xlsx"



def allo(dic_hive_depandances, dic_tables_hive_paths, dic_rdms_hive): 
    """
    Met à jour le dictionnaire des dépendances Hive en ajoutant de nouvelles clés 
    pour les tables Hive correspondantes aux tables RDMS, si elles n'existent pas déjà.

    Args:
        dic_hive_depandances (dict): 
            Dictionnaire des dépendances Hive. 
            Clés : noms des tables Hive. 
            Valeurs : listes de dépendances associées (tables RDMS ou Hive).
            
            Exemple :
            {
                "AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY": ["MON.FT_REFILL", "MON.FT_MISSING"]
            }
        
        dic_tables_hive_paths (dict): 
            Dictionnaire associant les tables Hive à leurs chemins de fichiers HQL.
            Clés : noms des tables Hive. 
            Valeurs : chemins des fichiers.
            
            Exemple :
            {
                "MON.SPARK_FT_REFILL": "/path/to/file1",
                "MON.SPARK_FT_OTHER": "/path/to/file2"
            }
        
        dic_rdms_hive (dict): 
            Dictionnaire associant des chemins de fichiers à des correspondances RDMS -> Hive.
            Clés : chemins de fichiers (str).
            Valeurs : dictionnaires avec les clés "table_data_rdms" (list) et "table_data_hive" (list).
            
            Exemple :
            {
                "path1": {"table_data_rdms": ["MON.FT_REFILL"], "table_data_hive": ["MON.SPARK_FT_REFILL"]},
                "path2": {"table_data_rdms": ["MON.FT_OTHER"], "table_data_hive": ["MON.SPARK_FT_OTHER"]}
            }

    Returns:
        dict: 
            Dictionnaire mis à jour des dépendances Hive. 
            Les nouvelles tables Hive trouvées sont ajoutées comme clés, avec une liste vide comme valeur.
            
            Exemple de sortie :
            {
                "AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY": ["MON.FT_REFILL", "MON.FT_MISSING"],
                "MON.SPARK_FT_REFILL": []
            }

    Fonctionnement :
    - Parcourt les dépendances de chaque table Hive dans `dic_hive_depandances`.
    - Pour chaque dépendance (table RDMS) :
        1. Recherche son équivalent Hive dans `dic_rdms_hive`.
        2. Si un équivalent Hive est trouvé et qu'il n'est pas déjà une clé dans `dic_hive_depandances` :
           - Ajoute cette table Hive comme nouvelle clé avec une liste vide comme dépendance initiale.
    - Renvoie le dictionnaire mis à jour.

    Exemple d'utilisation :
    >>> dic_hive_depandances = {
            "AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY": ["MON.FT_REFILL", "MON.FT_MISSING"]
        }
    >>> dic_tables_hive_paths = {
            "MON.SPARK_FT_REFILL": "/path/to/file1",
            "MON.SPARK_FT_OTHER": "/path/to/file2"
        }
    >>> dic_rdms_hive = {
            "path1": {"table_data_rdms": ["MON.FT_REFILL"], "table_data_hive": ["MON.SPARK_FT_REFILL"]},
            "path2": {"table_data_rdms": ["MON.FT_OTHER"], "table_data_hive": ["MON.SPARK_FT_OTHER"]}
        }
    >>> updated_dic = allo(dic_hive_depandances, dic_tables_hive_paths, dic_rdms_hive)
    >>> print(updated_dic)
    {
        "AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY": ["MON.FT_REFILL", "MON.FT_MISSING"],
        "MON.SPARK_FT_REFILL": []
    }
    """
    liste_path_hive = []
    
    # Parcourir les dépendances Hive
    for table_hive, dependencies in dic_hive_depandances.items():
        if table_hive == "AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY":
            print(f"Table Hive principale : {table_hive}")
            
            # Parcourir les dépendances de cette table
            for table_rdms in dependencies:
                if "spark" not in table_rdms.lower():
                    print(f"\nTable RDMS actuelle : {table_rdms}")
                    
                    # Rechercher la correspondance RDMS -> Hive dans dic_rdms_hive
                    found = False
                    for path, value in dic_rdms_hive.items():
                        rdms_tables = value.get("table_data_rdms", [])
                        hive_tables = value.get("table_data_hive", [])
                        
                        # Vérifier si la table RDMS est dans les clés correspondantes
                        if table_rdms in rdms_tables:
                            print(f"Équivalent trouvé : {table_rdms} -> {hive_tables}")
                            
                            # Vérifier si les tables Hive existent déjà dans dic_hive_depandances
                            for hive_table in hive_tables:
                                if hive_table not in dic_hive_depandances:
                                    # Ajouter la nouvelle clé avec une liste vide
                                    dic_hive_depandances[hive_table] = []
                                    print(f"Nouvelle clé ajoutée : {hive_table} avec une liste vide.")
                                
                            found = True
                            break
                    
                    if not found:
                        print(f"Aucune correspondance trouvée pour {table_rdms} dans dic_rdms_hive")
                        
    return dic_hive_depandances




l=allo(dic_hive_depandances,dic_tables_hive_paths,dic_rdms_hive)

     

# Appel de la fonction et affichage des résultats


for i, value in l.items():
    if i == "AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY":
        print("table hive", i)
        print("tables", value)

                


    #generate_excel_with_rdms_and_dependencies(dic_rdms_hive,dic_hive_depandances,output_file)
    #dependency_chain = build_dependency_chain(dic_rdms_hive, dic_hive_depandances)
    # Affichage du dictionnaire de dépendances avant l'aplatissement

#print("Dictionnaire de dépendances avant l'aplatissement:")


    #flattened_dependencies = flatten_dependencies(dependency_chain,dic_hive_depandances)
    #print_dependencies(dependency_chain, dic_hive_depandances)
    

    #dic_rdm_hives=extract_hive_table_and_queries(conf_dir)
    #generate_excel_with_rdms_and_dependencies(dic_hive_rdms, dic_hive_depandances, "output_file_with_cycles.xlsx")


    #for i, value in dic_tables_hive_paths.items():
    #    print(i,"table","path",value)
    #dependency_map = extract_table_names_from_load_conf_files(file_queries)
    # Afficher les résultats

       
    #output_file = 'wanda_moi.xlsx'
    #generate_excel(dependency_map, output_file)