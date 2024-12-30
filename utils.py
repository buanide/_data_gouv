import os
import re
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from pathlib import Path
import glob

def list_all_files(directory):
    """
    Retourne tous les chemins de fichiers dans un répertoire, y compris les sous-répertoires.
    """
    
    file_paths = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return file_paths



def list_files_glob(directory='.', pattern='**/*', recursive=True):
    """
    Liste tous les fichiers correspondant à un motif spécifié dans un répertoire donné.

    Args:
        directory (str): Le répertoire de base pour la recherche.
        pattern (str): Le motif de recherche de fichiers.
        recursive (bool): Indique si la recherche doit être récursive.

    Returns:
        list: Liste des chemins de fichiers trouvés.
    """
    # Construire le motif complet
    full_pattern = os.path.join(directory, pattern)
    
    # Utiliser glob pour trouver les fichiers correspondant au motif
    files = glob.glob(full_pattern, recursive=recursive)
    
    # Afficher les fichiers trouvés
    for file in files:
        print(file)
    
    return files

# Appel de la fonction avec le motif par défaut



# Specify the directory path you want to start from


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

def extract_tables_from_queries(queries):
    """
    Extrait les noms des tables de la forme DOMAINE.NOM_TABLE des requêtes SQL.

    Retourne: Une liste des noms des tables extraites.
    """
    # Motif pour trouver les tables dans les clauses FROM et JOIN
    table_pattern = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b'
    join_pattern = r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b'

    # Combiner les motifs pour trouver toutes les tables
    tables = re.findall(table_pattern, queries, re.IGNORECASE) + re.findall(join_pattern, queries, re.IGNORECASE)

    # Normaliser les noms des tables en majuscules
    tables = [table.upper() for table in tables]

    # Supprimer les doublons
    tables = list(set(tables))

    return tables




def extract_hive_table_and_queries(conf_dir):
    """"
  permet d'extraire pour chaque fichier conf la table rdms et hive
  dans les fichiers sqoop
    """
    results = {}
    #total_rdms_tables = set()
    #total_hive_tables = set()

    try:
        for root, dirs, files in os.walk(conf_dir):
            for file in files:
                if file.lower().startswith('sqoop-export-spark') and file.lower().endswith('.conf'):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                    except Exception as e:
                        print(f"Erreur lors de la lecture du fichier {file_path}: {e}")
                        continue

                    rdms_pattern = re.compile(r'flux\.rdms\.pre-exec-queries\s*\+=\s*""".*?FROM\s+(\S+)\s+WHERE', re.IGNORECASE | re.DOTALL)
                    rdms_match = rdms_pattern.search(content)
                    if rdms_match:
                        rdms_table = rdms_match.group(1)
                        hive_pattern = re.compile(r'flux\.hive\.pre-exec-queries\s*\+=\s*""".*?FROM\s+(\S+)\s+WHERE', re.IGNORECASE | re.DOTALL)
                        hive_match = hive_pattern.search(content)

                        if hive_match:
                            hive_table = hive_match.group(1)
                        else:
                            print(f"Aucune table Hive trouvée dans {file_path}")
                            hive_table = None
                    else:
                        print(f"Aucune table RDMS trouvée dans {file_path}")
                        rdms_table = None
                        hive_table = None

                    # Extraire les tables des requêtes SQL
                    tables_rdms = extract_tables_from_queries(rdms_match.group(0) if rdms_match else "")
                    tables_hive = extract_tables_from_queries(hive_match.group(0) if hive_match else "")

                    results[file_path] = {
                        "table_data_rdms": tables_rdms,
                        "table_data_hive": tables_hive
                    }

                    # Ajouter les tables aux ensembles globaux
                    #total_rdms_tables.update(tables_rdms)
                    #total_hive_tables.update(tables_hive)
    except Exception as e:
        print(f"Erreur lors de la recherche des fichiers dans {conf_dir}: {e}")
    return results


def extract_exec_queries(file_path):
    """
    Extrait les valeurs des variables flux.exec-queries et flux.pre-exec-queries dans un fichier de configuration.
    Args:
        file_path (str): Chemin du fichier de configuration.
    Returns:
        tuple: Deux listes contenant respectivement les valeurs de flux.pre-exec-queries et flux.exec-queries.
    """
    pre_exec_queries = []
    exec_queries = []

    try:
        with open(file_path, 'r') as file:
            content = file.read()

            # Expression régulière pour flux.pre-exec-queries
            pre_exec_matches = re.findall(r'flux\.pre-exec-queries\s*\+=\s*"([^"]+)"', content)
            if pre_exec_matches:
                pre_exec_queries.extend(query.strip() for query in pre_exec_matches)

            # Expression régulière pour flux.exec-queries
            exec_matches = re.findall(r'flux\.exec-queries\s*\+=\s*"([^"]+)"', content)
            if exec_matches:
                exec_queries.extend(query.strip() for query in exec_matches)

    except Exception as e:
        print(f"Erreur lors du traitement du fichier {file_path}: {e}")

    return pre_exec_queries, exec_queries



def process_conf_files(directory,hdfs_directory):
    """
    Traite les fichiers de configuration dans un répertoire donné et construit les chemins complets pour les requêtes pré-exécution et exécution.

    Args:
        directory (str): Le chemin du répertoire contenant les fichiers de configuration.
        hdfs_directory (str): Le chemin de base du répertoire HDFS.

    Returns:
        dict: Un dictionnaire où les clés sont les chemins des fichiers de configuration et les valeurs sont des dictionnaires contenant
              les listes de chemins complets pour les requêtes pré-exécution ('pre_exec') et exécution ('exec').
      """
    dic_conf_queries={}
    for root, dirs, files in os.walk(directory):
        for file in files:
            path = os.path.join(root, file)
            pre_exec_queries, exec_queries = extract_exec_queries(path)
            paths_pre_exec_queries = []
            path_exec_queries=[]
            if pre_exec_queries:
                for query in pre_exec_queries:
                    # Construire le chemin absolu
                    if query.startswith('/'):
                        query_windows_path = query.replace('/', '\\')
                        # Construire le chemin complet
                        full_path = hdfs_directory+query_windows_path
                        #print("Première partie du chemin :", hdfs_directory)
                        #print("Pre-exec query :", query)
                        #print("Chemin complet :", full_path)
                        paths_pre_exec_queries.append(full_path)
                    else:
                        print("ce n'est pas un chemin, fonction,process_conf_files", query)

            if exec_queries:
                for query in exec_queries:
                    # Construire le chemin absolu
                    if query.startswith('/'):
                        query_windows_path = query.replace('/', '\\')
                        # Construire le chemin complet
                        full_path = hdfs_directory+query_windows_path
                        #print("Première partie du chemin :", hdfs_directory)
                        #print("Pre-exec query :", query)
                        #print("Chemin complet :", full_path)
                        path_exec_queries.append(full_path)
                    else:
                        print("ce n'est pas un chemin, fonction,process_conf_files", query)

            dic_conf_queries[path] = {'pre_exec': paths_pre_exec_queries, 'exec': path_exec_queries}

    return dic_conf_queries
                 


def map_rdms_file_hql_file(dic_rdms_hive, list_paths_scripts_hql):
    """
    Retourne un dictionnaire avec en clé le nom de la table Hive et en valeur une liste des chemins des fichiers HQL associés.
    
    Args:
    - dic_rdms_hive (dict): Dictionnaire avec en clé le nom de la table rdms et en valeur un dictionnaire contenant "table_data_hive" 
      (une liste avec le nom de la table Hive en position 0).
    - list_paths_scripts_hql (list): Liste des chemins de tous les fichiers HQL.
    
    Returns:
    - dict: Un dictionnaire avec en clé le nom de la table Hive (str) et en valeur une liste des chemins de fichiers HQL (list).
    """
    dic = {}

    # Préparer le pattern pour détecter les instructions INSERT INTO
    insert_into_pattern = r'\bINSERT\s+INTO\s+(?:TABLE\s+)?([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)(?:\s+PARTITION\s*\([^\)]*\))?'

    # Étape 1 : Initialiser le dictionnaire avec les tables de `dic_rdms_hive`
    for key, value in dic_rdms_hive.items():
        if "table_data_hive" in value and value["table_data_hive"]:
            table_hive = value["table_data_hive"][0].upper()
            dic[table_hive] = []  # Initialiser une liste vide pour chaque table trouvée

    # Étape 2 : Parcourir les fichiers HQL
    for file_path in list_paths_scripts_hql:
        if "insert" in file_path.lower() or "compute" in file_path.lower():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    hql_content = f.read()
            except Exception as e:
                print(f"Erreur lors de la lecture du fichier {file_path}: {e}")
                continue

            # Rechercher les tables dans les instructions INSERT INTO
            insert_into_tables = re.findall(insert_into_pattern, hql_content, re.IGNORECASE)

            # Ajouter chaque table trouvée au dictionnaire
            for main_table in insert_into_tables:
                main_table_upper = main_table.upper()  # Uniformiser les noms de table en majuscules

                # Si la table existe déjà dans le dictionnaire, ajouter le chemin
                if main_table_upper in dic:
                    if file_path not in dic[main_table_upper]:
                        dic[main_table_upper].append(file_path)
                else:
                    dic[main_table_upper] = [file_path]  # Ajouter une nouvelle table avec son chemin
    
    return dic


# à améliorer avec une librairie qui parse les requêtes sql

def extract_data_sources(hql_file_path):
    """
    Permet de lire un fichier HQL et d'en extraire ses tables.

    Retourne: la table principale et ses tables dépendantes.
    """
    try:
        # Lire le contenu du fichier HQL
        with open(hql_file_path, 'r') as file:
            hql_content = file.read()
    except FileNotFoundError:
        print(f"Erreur : Le fichier '{hql_file_path}' n'a pas été trouvé, fonction extract_data_sources")
        return [], None

    # Utiliser des expressions régulières pour trouver les sources de données
    # Exemple de motifs pour les tables de la forme DOMAINE.NOM_TABLE
    table_pattern = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b'
    join_pattern = r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b'
    database_pattern = r'\bUSE\s+([a-zA-Z_][a-zA-Z0-9_]*)\b'
    insert_into_pattern = r'\bINSERT\s+INTO\s+(?:(TABLE\s+)?([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)(?:\s+PARTITION\s*\([a-zA-Z_][a-zA-Z0-9_]*\))?)\b'
    
# Recherche des correspondances
    
    # Trouver toutes les occurrences des motifs
    tables_from = re.findall(table_pattern, hql_content, re.IGNORECASE)
    tables_join = re.findall(join_pattern, hql_content, re.IGNORECASE)
    insert_into_tables = re.findall(insert_into_pattern, hql_content, re.IGNORECASE)
    #insert_into_tables_flux_in_line = re.findall(insert_into_patter_flux_in_line, hql_content,re.IGNORECASE)

    # Combiner les tables trouvées dans FROM et JOIN
    tables = tables_from + tables_join

    # Normaliser les noms des tables en majuscules
    tables = [table.upper() for table in tables]

    # Supprimer les doublons
    tables = list(set(tables))

    # Identifier la table principale
    main_table = None
    if insert_into_tables:
        for match in insert_into_tables:
            if match[1]:  # Utiliser la deuxième capture (le nom de la table)
                main_table = match[1].upper()
                break

    # Enlever la table principale de la liste des tables
    if main_table and main_table in tables:
        tables.remove(main_table)

    return tables, main_table


def extract_tables_from_hql(dic_name_table_hql_path):
    """
    dic_name_table_hql_path (dict): Dictionnaire avec en clé le nom de la table Hive (str) 
    et en valeur une liste des chemins des fichiers HQL (list) associés.
    
    Extrait les dépendances des tables du datalake à partir des fichiers HQL.
    
    Retourne:
    - dict: Un dictionnaire avec en clé le nom de la table principale Hive (str) 
            et en valeur une liste de ses dépendances (list).
    """
    dic_load = {}

    for table, paths in dic_name_table_hql_path.items():
        all_dependencies = set()  # Pour éviter les doublons dans les dépendances

        for path in paths:
            try:
                # Extraire les dépendances à partir du fichier HQL
                dependances, main_table = extract_data_sources(path)

                # Vérifier que le fichier HQL correspond bien à la table en cours
                if main_table.upper() == table.upper():
                    all_dependencies.update(dependances)
            except Exception as e:
                print(f"Erreur lors de l'extraction des dépendances depuis {path}: {e}")

        dic_load[table] = list(all_dependencies)  # Convertir l'ensemble en liste

    return dic_load


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


def generate_excel_with_rdms_and_dependencies_3(results, dependency_map, output_file):
    """
    Génère un fichier Excel avec les relations RDMS -> Hive et leurs dépendances Hive, jusqu'à ce qu'il n'y ait plus de dépendances directes.
    Args:
        results (dict): Dictionnaire contenant les associations RDMS et Hive.
        dependency_map (dict): Dictionnaire des dépendances {table_hive: [dépendances]}.     
        output_file (str): Chemin du fichier Excel de sortie.
    """
    # Liste pour stocker les chemins uniques de dépendances
    unique_paths = set()

    def get_all_dependencies(table, current_path, visited):
        """
        Explore toutes les dépendances d'une table en profondeur et ajoute chaque chemin unique.

        Args:
            table (str): La table pour laquelle les dépendances doivent être explorées.
            current_path (list): Le chemin courant (accumulé).
            visited (set): Ensemble des tables déjà visitées pour éviter les cycles.
        """
        if table in visited:
            # Cycle détecté, ajouter le chemin avec une indication
            unique_paths.add(tuple(current_path + [f"{table} (cycle détecté)"]))
            return

        # Ajouter la table courante au chemin
        current_path = current_path + [table]

        # Si la table n'a pas de dépendances, ajouter le chemin complet
        if table not in dependency_map or not dependency_map[table]:
            unique_paths.add(tuple(current_path))
            return

        # Marquer la table comme visitée
        visited.add(table)

        # Parcourir les dépendances et continuer l'exploration
        for dependency in dependency_map[table]:
            get_all_dependencies(dependency, current_path, visited.copy())

    # Traitement des associations RDMS -> Hive et dépendances Hive
    for file_path, table_info in results.items():
        tables_rdms = table_info.get("table_data_rdms", [])
        tables_hive = table_info.get("table_data_hive", [])
        for rdms_table in tables_rdms:
            for hive_table in tables_hive:
                # Ajouter la relation RDMS -> Hive comme point de départ
                unique_paths.add((rdms_table, hive_table))

                # Ajouter les dépendances Hive pour cette table Hive
                if hive_table in dependency_map:
                    get_all_dependencies(hive_table, [rdms_table], set())

    # Convertir les chemins uniques en lignes pour le DataFrame
    rows = [list(path) for path in unique_paths]

    # Déterminer le nombre maximum de colonnes pour formater correctement le fichier Excel
    max_columns = max(len(row) for row in rows)
    columns = ["Table_RDMS", "Table_Hive"] + [f"Dep_datalake{i+1}" for i in range(max_columns - 2)]

    # Créer un DataFrame avec les données collectées
    df = pd.DataFrame(rows, columns=columns)

    df_unique=df.drop_duplicates()

    # Exporter le DataFrame vers un fichier Excel
    df_unique.to_excel(output_file, index=False)
    print(f"Fichier Excel généré avec succès : {output_file}")


def generate_excel_with_table_dependencies(dependency_map, output_file):
    """
    Génère un fichier Excel avec les dépendances des tables jusqu'à ce qu'il n'y ait plus de dépendances directes.

    Args:
        dependency_map (dict): Dictionnaire des dépendances {table_principale: [dépendances]}.
        output_file (str): Chemin du fichier Excel de sortie.
    """
    # Liste pour stocker les chemins uniques de dépendances
    unique_paths = set()

    def get_all_dependencies(table, current_path, visited):
        """
        Explore toutes les dépendances d'une table en profondeur et ajoute chaque chemin unique.

        Args:
            table (str): La table pour laquelle les dépendances doivent être explorées.
            current_path (list): Le chemin courant (accumulé).
            visited (set): Ensemble des tables déjà visitées pour éviter les cycles.
        """
        if table in visited:
            # Cycle détecté, ajouter le chemin avec une indication
            unique_paths.add(tuple(current_path + [f"{table} (cycle détecté)"]))
            return

        # Ajouter la table courante au chemin
        current_path = current_path + [table]

        # Si la table n'a pas de dépendances, ajouter le chemin complet
        if table not in dependency_map or not dependency_map[table]:
            unique_paths.add(tuple(current_path))
            return

        # Marquer la table comme visitée
        visited.add(table)

        # Parcourir les dépendances et continuer l'exploration
        for dependency in dependency_map[table]:
            get_all_dependencies(dependency, current_path, visited.copy())

    # Parcourir chaque table principale dans le dictionnaire
    for main_table in dependency_map:
        get_all_dependencies(main_table, [], set())

    # Convertir les chemins uniques en lignes pour le DataFrame
    rows = [list(path) for path in unique_paths]

    # Déterminer le nombre maximum de colonnes pour formater correctement le fichier Excel
    max_columns = max(len(row) for row in rows)
    columns = ["Table_Principale"] + [f"Dépendance{i+1}" for i in range(max_columns - 1)]

    # Créer un DataFrame avec les données collectées
    df = pd.DataFrame(rows, columns=columns)

    # Supprimer les doublons et exporter les données vers un fichier Excel
    df_unique = df.drop_duplicates()
    df_unique.to_excel(output_file, index=False)
    print(f"Fichier Excel généré avec succès : {output_file}")



def get_dir_dependances(dic_files_queries_paths):
    dic = {}
    for i, queries in dic_files_queries_paths.items():
        if queries['exec']:
            for q in queries['exec']:
                dependent_tables, main_table = extract_data_sources(q)
                if main_table and main_table not in dic:
                    dic[main_table] = set()
                if main_table:
                    dic[main_table].update(dependent_tables)

        if queries['pre_exec']:
            for q in queries['pre_exec']:
                dependent_tables, main_table = extract_data_sources(q)
                if main_table and main_table not in dic:
                    dic[main_table] = set()
                if main_table:
                    dic[main_table].update(dependent_tables)

    for table in dic:
        dic[table] = list(dic[table])

    return dic


def display_table_dependencies(dependency_map, table_name):
    """
    Affiche les dépendances d'une table spécifique DU DATALAKE et les écrit dans un fichier Excel si elles existent.

    Args:
        dependency_map (dict): Dictionnaire des dépendances {table_principale: [dépendances]}.
        table_name (str): Nom de la table pour laquelle afficher les dépendances.
    """
    # Liste pour stocker les chemins uniques de dépendances
    unique_paths = set()

    def get_all_dependencies(table, current_path, visited):
        """
        Explore toutes les dépendances d'une table en profondeur et ajoute chaque chemin unique.

        Args:
            table (str): La table pour laquelle les dépendances doivent être explorées.
            current_path (list): Le chemin courant (accumulé).
            visited (set): Ensemble des tables déjà visitées pour éviter les cycles.
        """
        if table in visited:
            # Cycle détecté, ajouter le chemin avec une indication
            unique_paths.add(tuple(current_path + [f"{table} (cycle détecté)"]))
            return

        # Ajouter la table courante au chemin
        current_path = current_path + [table]

        # Si la table n'a pas de dépendances, ajouter le chemin complet
        if table not in dependency_map or not dependency_map[table]:
            unique_paths.add(tuple(current_path))
            return

        # Marquer la table comme visitée
        visited.add(table)

        # Parcourir les dépendances et continuer l'exploration
        for dependency in dependency_map[table]:
            get_all_dependencies(dependency, current_path, visited.copy())

    # Vérifier si la table existe dans le dictionnaire
    if table_name in dependency_map:
        get_all_dependencies(table_name, [], set())
    else:
        print(f"La table '{table_name}' n'existe pas dans le dictionnaire.")
        return

    # Convertir les chemins uniques en lignes pour le DataFrame
    rows = [list(path) for path in unique_paths]

    # Déterminer le nombre maximum de colonnes pour formater correctement
    max_columns = max(len(row) for row in rows)
    columns = ["Table_Principale"] + [f"Dépendance{i+1}" for i in range(max_columns - 1)]

    # Créer un DataFrame avec les données collectées
    df = pd.DataFrame(rows, columns=columns)

    # Exporter les données vers un fichier Excel
    output_file = f"{table_name}_dependencies.xlsx"
    df.to_excel(output_file, index=False)
    print(f"Les dépendances de la table '{table_name}' ont été exportées vers : {output_file}")


def redirect_error(list_to_redirect):
    """
    Rediriger les erreurs vers un fichier
    """
    with open("output.txt", "w", encoding="utf-8") as fichier:
    # Ajouter une nouvelle ligne à chaque élément
        fichier.writelines(element + "\n" for element in list_to_redirect)

def create_dic_fil_queries(result):
    """
     Crée un dictionnaire contenant les chemins des fichiers en clé 
    et les chemins des fichiers de requêtes en valeur.

    Args:
        result (dict): Dictionnaire où chaque clé est un chemin de fichier 
                       et chaque valeur est un autre dictionnaire contenant 
                       potentiellement deux clés :
                       - 'pre_exec': Liste des requêtes de pré-exécution associées.
                       - 'exec': Liste des requêtes principales associées.

    Returns:
        dict: Un dictionnaire où chaque clé est un chemin de fichier et 
              chaque valeur est un dictionnaire contenant :
              - 'pre_exec': Liste des requêtes de pré-exécution, ou une liste vide si non présentes.
              - 'exec': Liste des requêtes principales, ou une liste vide si non présentes.
    
    """
    dic = {}
    for file_path, queries in result.items():
        dic[file_path] = {
            'pre_exec': queries.get('pre_exec', []),
            'exec': queries.get('exec', [])
        }
    return dic

def update_dependency_dict(existing_dict, dic_tables):
    """
    Met à jour un dictionnaire existant avec le contenu de dic_tables.
    - Si une clé existe, étend la liste des dépendances.
    - Si une clé n'existe pas (et n'est pas None), elle est ajoutée avec ses dépendances.
    Args:
        existing_dict (dict): Dictionnaire à mettre à jour.
        dic_tables (dict): Dictionnaire contenant les nouvelles données.

    Returns:
        dict: Dictionnaire mis à jour.
    """
    for key, value in dic_tables.items():
        # Vérifier que la clé n'est ni None ni vide
        if key:
            # Si la clé existe, étendre les dépendances
            if key in existing_dict:
                existing_dict[key]['tables_dépendantes'].extend(value['tables_dépendantes'])
                # Éviter les doublons dans les dépendances
                existing_dict[key]['tables_dépendantes'] = list(set(existing_dict[key]['tables_dépendantes']))
            else:
                # Ajouter la clé et ses dépendances au dictionnaire
                existing_dict[key] = value
    return existing_dict



def write_file_paths_to_txt(file_paths, output_file):
    """
    Écrit les chemins de fichiers dans un fichier texte.
    """
    try:
        with open(output_file, 'w') as f:
            for file_path in file_paths:
                f.write(file_path + '\n')
        print(f"Les chemins de fichiers ont été écrits dans {output_file}")
    except Exception as e:
        print(f"Erreur lors de l'écriture dans le fichier {output_file}: {e}")