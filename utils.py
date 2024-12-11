import os
import re

def list_all_files(directory):
    """
    Retourne tous les chemins de fichiers dans un répertoire, y compris les sous-répertoires.
    """
    print("a")
    file_paths = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return file_paths

def extract_pre_exec_and_exec_queries_by_file(file_paths, root_directory):
    """
    Associe chaque fichier à ses pré-queries en utilisant des chemins absolus.

    Args:
        file_paths (list): Liste des chemins (relatifs ou absolus) vers les fichiers de configuration.
        root_directory (str): Chemin racine à partir duquel reconstruire les chemins relatifs.

    Returns:
        dict: Dictionnaire où chaque clé est le chemin absolu du fichier 
              et la valeur est une liste des pré-queries sous forme de chemins absolus valides.
    """
    file_queries = {}

    for file_path in file_paths:
        # Convertir le chemin du fichier en chemin absolu
        absolute_path = os.path.abspath(file_path)
        queries = []
        try:
            with open(absolute_path, 'r', encoding='utf-8') as file:
                for line in file:
                    if "flux.exec-queries" in line:
                        # Extraction de la valeur après le "="
                        _, value = line.split("=", 1)
                        
                        # Nettoyage de la valeur pour supprimer les guillemets, les espaces, et les commentaires
                        clean_value = value.split("//")[0].strip().strip('"').strip()
                        
                        # Vérifier si la valeur nettoyée correspond à un chemin relatif
                        if clean_value.startswith("/"):
                            normalized_path = os.path.normpath(
                                os.path.join(root_directory, clean_value.lstrip("/"))
                            )
                            queries.append(normalized_path)
                        
            file_queries[absolute_path] = queries
        except Exception as e:
            print(f"Erreur avec le fichier {absolute_path}: {e}")
            file_queries[absolute_path] = None  # Indique une erreur pour ce fichier

    return file_queries

def extract_table_name_from_file(file_path):
    """
    Extrait le nom de la table d'une clause INSERT INTO à partir d'un fichier .hql.

    Args:
        file_path (str): Le chemin vers le fichier contenant la requête SQL.

    Returns:
        str: Le nom de la table extrait ou None si aucun nom de table n'est trouvé.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            query = file.read()
            # Expression régulière pour capturer le nom de la table après "INSERT INTO"
            match = re.search(r"INSERT\s+INTO\s+([^\s\(\)]+)", query, re.IGNORECASE)
            if match:
                return match.group(1)
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier {file_path}: {e}")
    return None


def extract_table_names_from_load_queries(file_queries):
    """
    extrait le nom des tables dans les requêtes de la form INSERT INTO
    pour les fichiers .conf possédants dans leur nom
    """ 
    tables=[]
    fichiers_conf=[]
    dic_load={}
    for key, value in file_queries.items():
        if "load" in key:
            for i in value:
             if "insert" in i:
                 dic_load[key]=extract_table_name_from_file(i)
    return dic_load
                      
if __name__ == "__main__":
    root_dir = "C:/Users/dokie/Downloads/wetransfer_hdfs_2024-12-09_1245/HDFS/HDFS"
    conf_dir = r"C:\Users\dokie\Downloads\wetransfer_hdfs_2024-12-09_1245\HDFS\HDFS\PROD\CONF" 
    chains = list_all_files(conf_dir)
    a=chains[0]
    result=extract_pre_exec_and_exec_queries_by_file(chains,root_dir)
    dic=extract_table_names_from_load_queries(result)

    for key, value in dic.items():
        print("conf",key)
        print("tables",value)



    
    
