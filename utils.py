import os
import re
import pandas as pd

def list_all_files(directory):
    """
    Retourne tous les chemins de fichiers dans un répertoire, y compris les sous-répertoires.
    """
    
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

def map_rdms_file_hql_file(dic_rdms_hive,list_paths_scripts_hql):
    """"
    
    retourne un dictionnaire avec en clé le nom de la table hive et en valeur le chemin de son hql
    dic_rdms_hive(dic): en clé le nom de la table et en valeur le chemin du fichier
    list_paths_scripts_hql(list): contient la liste de tous les scripts hql

    """  

    dic={}
    for key, value in dic_rdms_hive.items():
              for file_path in list_paths_scripts_hql:
                   if "insert" or "compute" in file_path:
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                        hql_content = f.read()
                        except Exception as e:
                            print(f"Erreur lors de la lecture du fichier {file_path}: {e}, function map_rdms_file_hql_file ")
                            return []
                        
                        insert_into_pattern = r'\bINSERT\s+INTO\s+(?:(TABLE\s+)?([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*))\b'
                        insert_into_tables = re.findall(insert_into_pattern, hql_content, re.IGNORECASE)
                        #print("chmin",i)
                        main_table = None
                        if insert_into_tables:
                            for match in insert_into_tables:
                                if match[1]:  # Utiliser la deuxième capture (le nom de la table)
                                    main_table = match[1].upper()
                                    break
                                     
                        if main_table and value["table_data_hive"]:
                            if main_table==value["table_data_hive"][0]: 
                            #print("ok")
                                dic[main_table]=file_path
        
    return dic
    
        

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
    insert_into_pattern = r'\bINSERT\s+INTO\s+(?:(TABLE\s+)?([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*))\b'

    # Trouver toutes les occurrences des motifs
    tables_from = re.findall(table_pattern, hql_content, re.IGNORECASE)
    tables_join = re.findall(join_pattern, hql_content, re.IGNORECASE)
    insert_into_tables = re.findall(insert_into_pattern, hql_content, re.IGNORECASE)

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
    dic_name_table_hql_path(dic): dictionnaire avec en clé le nom de la table HIVE et le chemin du .hql qui l'alimente
    extrait les dépendances d'une du datalake à partir du chemin de son .hql
    retourne un dictionnaire avec en clé le nom de la table principale hive et en valeurs ses dépendances 
    """
    dic_load={}
    for i,path in dic_name_table_hql_path.items():
        dependances,table=extract_data_sources(path)
        dic_load[table]=dependances

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




def generate_excel(dependency_map, output_file):
    """
    Génère un fichier Excel en ajoutant des lignes pour chaque dépendance,
    en progressant récursivement pour chaque niveau de dépendance.
    
    Args:
        dependency_map (dict): Dictionnaire des dépendances {table: [dépendances]}.
        output_file (str): Chemin du fichier Excel de sortie.
    """
    if not dependency_map:
        print("Aucune dépendance trouvée.")
        return

    # Liste pour stocker les lignes du tableau final
    rows = []

    def process_table(table, dependency_path, visited_tables):
        """
        Récursivement, ajoute les dépendances dans le tableau final.
        
        Args:
            table (str): Table principale ou dépendance à traiter.
            dependency_path (list): Chemin hiérarchique des dépendances.
            visited_tables (set): Ensemble des tables déjà visitées pour éviter les boucles.
        """
        # Si la table est déjà visitée, éviter la récursion infinie
        if table in visited_tables:
            print(f"Avertissement : Cycle détecté pour la table '{table}'. Ignoré.")
            return
        
        # Ajouter la table actuelle au chemin de dépendance
        visited_tables.add(table)

        # Ajouter une ligne pour la table courante et son chemin de dépendance
        row = dependency_path + [table]
        rows.append(row)
        
        # Si la table a des dépendances, continuer récursivement
        if table in dependency_map:
            for dep in dependency_map[table]:
                process_table(dep, row, visited_tables.copy())

    # Parcourir chaque table principale dans le dictionnaire
    for main_table in dependency_map:
        process_table(main_table, [], set())

    # Trouver le niveau maximum de dépendance pour ajuster les colonnes
    max_depth = max(len(row) for row in rows)
    columns = [f"Dep_datalake{i+1}" for i in range(max_depth)]

    # Créer le DataFrame final
    df = pd.DataFrame(rows, columns=columns)
    df.to_excel(output_file, index=False)

def generate_excel_with_rdms_and_dependencies_2(rdms_hive_map, dependency_map, output_file):
    """
    Génère un fichier Excel avec les relations RDMS -> Hive et leurs dépendances Hive, cycles inclus.

    Args:
        rdms_hive_map (dict): Dictionnaire contenant les associations RDMS et Hive.
        dependency_map (dict): Dictionnaire des dépendances {table_hive: [dépendances]}.
        output_file (str): Chemin du fichier Excel de sortie.
    """
    # Liste pour stocker les lignes de données
    rows = []

    def process_table(table, dependency_path, visited_tables):
        """
        Récursivement, ajoute les dépendances Hive dans la liste des lignes, y compris les cycles.

        Args:
            table (str): Table principale ou dépendance à traiter.
            dependency_path (list): Chemin des dépendances accumulées.
            visited_tables (set): Ensemble des tables déjà visitées pour éviter les boucles.
        """
        # Si la table est déjà visitée, éviter la récursion infinie
        if table in visited_tables:
            return

        # Ajouter la table actuelle au chemin de dépendance
        visited_tables.add(table)

        # Ajouter une ligne pour la table courante et son chemin de dépendance
        current_row = dependency_path + [table]
        rows.append(current_row)

        # Si la table a des dépendances, continuer récursivement
        for i, value in dependency_map.items():
            if table==i:
                for dep in dependency_map[table]:
                    process_table(dep, current_row, visited_tables.copy())

    # Traitement des associations RDMS -> Hive et dépendances Hive
    for rdms_table, hive_table in rdms_hive_map.items():
        # Ajouter la relation RDMS -> Hive en tant que ligne de base
        rows.append([rdms_table, hive_table])

        # Ajouter les dépendances Hive pour cette table Hive
        for table_hive, dependances in dependency_map.items():
            if hive_table == table_hive:
                process_table(hive_table, dependances, set())

    # Déterminer le nombre maximum de colonnes pour formater correctement le fichier Excel
    max_columns = max(len(row) for row in rows)
    columns = ["Table_RDMS", "Table_Hive"] + [f"Dep_datalake{i+1}" for i in range(max_columns - 2)]

    # Créer un DataFrame avec les données collectées
    df = pd.DataFrame(rows, columns=columns)

    # Exporter le DataFrame vers un fichier Excel
    df.to_excel(output_file, index=False)
    print(f"Fichier Excel généré avec succès : {output_file}")




def generate_excel_with_rdms_and_dependencies(results, dependency_map, output_file):
    """
    Génère un fichier Excel avec les relations RDMS -> Hive et leurs dépendances Hive, cycles inclus.

    Args:
        results (dict): Dictionnaire contenant les associations RDMS et Hive.
        dependency_map (dict): Dictionnaire des dépendances {table_hive: [dépendances]}.
        output_file (str): Chemin du fichier Excel de sortie.
    """
    # Liste pour stocker les lignes de données
    rows = []

    def process_table(table, dependency_path):
        """
        Récursivement, ajoute les dépendances Hive dans la liste des lignes, y compris les cycles.

        Args:
            table (str): Table principale ou dépendance à traiter.
            dependency_path (list): Chemin des dépendances accumulées.
        """
        # Crée une nouvelle ligne avec la dépendance actuelle
        current_row = dependency_path + [table]
        rows.append(current_row)
        
        # Continuer avec les dépendances si elles existent
        if table in dependency_map:
            for dep in dependency_map[table]:
                process_table(dep, current_row)  # Pas de vérification des cycles ici

    # Traitement des associations RDMS -> Hive et dépendances Hive
    for file_path, table_info in results.items():
        tables_rdms = table_info.get("table_data_rdms", [])
        tables_hive = table_info.get("table_data_hive", [])
        for rdms_table in tables_rdms:
            for hive_table in tables_hive:
                # Ajouter la relation RDMS -> Hive en tant que ligne de base
                rows.append([rdms_table, hive_table])
                
                # Ajouter les dépendances Hive pour cette table Hive
                if hive_table in dependency_map:
                    process_table(hive_table, [rdms_table])

    # Déterminer le nombre maximum de colonnes pour formater correctement le fichier Excel
    max_columns = max(len(row) for row in rows)
    columns = ["Table_RDMS", "Table_Hive"] + [f"Dep_datalake{i+1}" for i in range(max_columns - 2)]
    
    # Créer un DataFrame avec les données collectées
    df = pd.DataFrame(rows, columns=columns)
    
    # Exporter le DataFrame vers un fichier Excel
    df.to_excel(output_file, index=False)
    print(f"Fichier Excel généré avec succès : {output_file}")
