import os
import re
import pandas as pd

def list_all_files(directory):
    """
    Retourne tous les chemins de fichiers dans un répertoire, y compris les sous-répertoires.
    """
    print("a")
    print("repertoire",directory)
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


def extract_data_sources(hql_file_path):
    """

    permet de lire un fichier hql et d'en extraire ces tables

    retourne: la table principale et ses tables dépendantes
    """
    try:
        # Lire le contenu du fichier HQL
        with open(hql_file_path, 'r') as file:
            hql_content = file.read()
    except FileNotFoundError:
        print(f"Erreur : Le fichier '{hql_file_path}' n'a pas été trouvé.")
        return [], None

    # Utiliser des expressions régulières pour trouver les sources de données
    # Exemple de motifs pour les tables de la forme DOMAINE.NOM_TABLE
    table_pattern = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b'
    join_pattern = r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b'
    database_pattern = r'\bUSE\s+([a-zA-Z_][a-zA-Z0-9_]*)\b'
    insert_into_pattern = r'\bINSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b'

    # Trouver toutes les occurrences des motifs
    tables_from = re.findall(table_pattern, hql_content, re.IGNORECASE)
    tables_join = re.findall(join_pattern, hql_content, re.IGNORECASE)
    insert_into_tables = re.findall(insert_into_pattern, hql_content, re.IGNORECASE)

    # Combiner les tables trouvées dans FROM et JOIN
    tables = tables_from + tables_join

    # Supprimer les doublons
    tables = list(set(tables))
    

    # Identifier la table principale
    main_table = insert_into_tables[0] if insert_into_tables else None

    if main_table and main_table in tables:
        tables.remove(main_table)

    return tables, main_table

def find_dependencies(hql_file_path, dependency_map,list_paths_conf_files):
    """
    Fonction récursive pour trouver les dépendances de chaque table dépendante.

    Retourne: Un dictionnaire de dépendances.
    """
    tables, main_table = extract_data_sources(hql_file_path)

    if main_table:
        dependency_map[main_table] = tables
        for table in tables:
            for i in list_paths_conf_files:
                    
                dependent_file_path = f'path/to/your/{table}.hql'  # Remplacez par le chemin réel
                if os.path.exists(dependent_file_path):
                    find_dependencies(dependent_file_path, dependency_map)

    return dependency_map


def extract_table_names_from_load_conf_files(file_queries):
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
                 #print("chmin",i)
                 dependances,table=extract_data_sources(i)
                 dic_load[table]=dependances

    return dic_load

def find_dependencies_(hql_file_path, dependency_map, file_queries):
    """
    Fonction récursive pour trouver les dépendances de chaque table dépendante.

    Retourne: Un dictionnaire de dépendances.
    """
    tables, main_table = extract_data_sources(hql_file_path)

    if main_table:
        dependency_map[main_table] = tables
        for table in tables:
            # Rechercher le fichier HQL correspondant à la table dépendante
            dependent_file_path = None
            for query_files in file_queries.values():
                for query_file in query_files:
                    with open(query_file, 'r') as file:
                        content = file.read()
                        recherche=re.search(r'\bINSERT\s+INTO\s+' + re.escape(table) + r'\b', content, re.IGNORECASE)
                        print("recherche de l'expression:",recherche)
                        if re.search(r'\bINSERT\s+INTO\s+' + re.escape(table) + r'\b', content, re.IGNORECASE):
                            dependent_file_path = query_file
                            break
                if dependent_file_path:
                    break

            if dependent_file_path and os.path.exists(dependent_file_path):
                find_dependencies(dependent_file_path, dependency_map, file_queries)

    return dependency_map

def extract_table_names_from_load_conf_files_(file_queries):
    """
    Extrait le nom des tables dans les requêtes de la forme INSERT INTO
    pour les fichiers .conf possédant 'load' dans leur nom.
    """
    dic_load = {}
    for key, value in file_queries.items():
        if "load" in key:
            for query_file in value:
                if "insert" in query_file.lower():
                    dependencies, main_table = extract_data_sources(query_file)
                    if main_table:
                        dic_load[main_table] = dependencies
                        # Trouver les dépendances récursives
                        dependency_map = {}
                        find_dependencies(query_file, dependency_map, file_queries)
                        dic_load[main_table].extend(dependency_map.get(main_table, []))

    return dic_load



def generate_excel(dependency_map, output_file):
    """
    Génère un fichier Excel à partir du dictionnaire de dépendances.
    """
    data = []
    for main_table, dependencies in dependency_map.items():
        for dependency in dependencies:
            data.append([main_table, dependency])

    df = pd.DataFrame(data, columns=["Table Principale", "Dépendance"])
    df.to_excel(output_file, index=False)
                      




    
    
