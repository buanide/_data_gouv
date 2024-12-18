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

    #for i, value in dic_tables_hive_paths.items():
     #   print("table",i)



    #output_file = "dependencies_2.xlsx"

    generate_excel_with_rdms_and_dependencies(dic_rdms_hive, dic_hive_depandances, "output_file_with_cycles.xlsx")

def allo(dic_hive_depandances, dic_tables_hive_paths, dic_rdms_hive): 
    """
    Met à jour le dictionnaire des dépendances Hive. Ajoute de nouvelles tables en tant que clés, 
    extrait les dépendances depuis leurs fichiers HQL si disponibles, ou laisse la liste vide.

    Args:
        dic_hive_depandances (dict): Dictionnaire des dépendances Hive.
        dic_tables_hive_paths (dict): Dictionnaire associant les tables Hive à leurs chemins de fichiers HQL.
        dic_rdms_hive (dict): Dictionnaire associant des chemins de fichiers à des correspondances RDMS -> Hive.
       
    Returns:
        dict: Dictionnaire mis à jour des dépendances Hive.
    """
    # Créer une copie des clés du dictionnaire pour éviter des erreurs de modification pendant l'itération
    keys_copy = list(dic_hive_depandances.keys())
    
    for table_hive in keys_copy:
        dependencies = dic_hive_depandances[table_hive]

        for i in dependencies:
            if "spark" not in i.lower():
                print(f"\nTable actuelle : {i}")
                correspondance_trouvee = False  # Flag pour vérifier si une correspondance est trouvée
                
                # Rechercher la correspondance RDMS -> Hive
                for path, value in dic_rdms_hive.items():
                    rdms_tables = value.get("table_data_rdms", [])
                    hive_tables = value.get("table_data_hive", [])
                    
                    if i in rdms_tables:
                        correspondance_trouvee = True
                        print(f"Équivalent trouvé : {i} -> {hive_tables}")
                        
                        for hive_table in hive_tables:
                            if hive_table not in dic_hive_depandances:
                                dic_hive_depandances[hive_table] = []
                                print(f"Nouvelle clé ajoutée : {hive_table} avec une liste vide.")
                
                if not correspondance_trouvee:
                    # Ajouter la table hive elle-même comme clé si elle n'existe pas
                    if i not in dic_hive_depandances:
                        dic_hive_depandances[i] = []
                        print(f"Table sans correspondance ajoutée : {i} avec une liste vide.")
                    
                    # Rechercher le fichier HQL associé à cette table
                    if i in dic_tables_hive_paths:
                        hql_file_path = dic_tables_hive_paths[i]
                        print(f"Chemin HQL trouvé pour {i} : {hql_file_path}")
                        
                        # Utiliser la fonction extract_data_sources pour extraire les dépendances
                        _, dependent_tables = extract_data_sources(hql_file_path)
                        
                        # Ajouter les tables dépendantes à la liste
                        dic_hive_depandances[i].extend(dependent_tables)
                        print(f"Dépendances ajoutées pour {i} : {dependent_tables}")
                    else:
                        print(f"Aucun fichier HQL trouvé pour {i}. La liste reste vide.")
    
    return dic_hive_depandances

    



#l=allo(dic_hive_depandances,dic_tables_hive_paths,dic_rdms_hive)
# Appel de la fonction et affichage des résultats




    #generate_excel_with_rdms_and_dependencies(dic_rdms_hive,dic_hive_depandances,output_file)
    #dependency_chain = build_dependency_chain(dic_rdms_hive, dic_hive_depandances)
    # Affichage du dictionnaire de dépendances avant l'aplatissement

#print("Dictionnaire de dépendances avant l'aplatissement:")


    #flattened_dependencies = flatten_dependencies(dependency_chain,dic_hive_depandances)
    #print_dependencies(dependency_chain, dic_hive_depandances)
    

    #dic_rdm_hives=extract_hive_table_and_queries(conf_dir)
    

    #for i, value in dic_tables_hive_paths.items():
    #    print(i,"table","path",value)
    #dependency_map = extract_table_names_from_load_conf_files(file_queries)
    # Afficher les résultats

       
    #output_file = 'wanda_moi.xlsx'
    #generate_excel(dependency_map, output_file)