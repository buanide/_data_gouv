import os
import re
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

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


def extract_dependencies_from_conf(conf_dir):
    """"
  permet d'extraire pour chaque fichier cses éventuelles dépendances
    """
    results = {}
    #total_rdms_tables = set()
    #total_hive_tables = set()

    try:
        for root, dirs, files in os.walk(conf_dir):
            for file in files:
                if not file.lower().startswith('sqoop-export-spark') and file.lower().endswith('.conf'):
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


def map_rdms_file_hql_file(dic_rdms_hive, list_paths_scripts_hql):
    """
    Retourne un dictionnaire avec en clé le nom de la table Hive et en valeur une liste des chemins des fichiers HQL associés.
    
    Args:
    - dic_rdms_hive (dict): Dictionnaire avec en clé un identifiant et en valeur un dictionnaire contenant "table_data_hive" 
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



def generate_excel_with_rdms_and_dependencies(results, dependency_map, output_file):
    """
    Génère un fichier Excel avec les relations RDMS -> Hive et leurs dépendances Hive, en évitant les cycles.

    Args:
        results (dict): Dictionnaire contenant les associations RDMS et Hive.
        dependency_map (dict): Dictionnaire des dépendances {table_hive: [dépendances]}.     
        output_file (str): Chemin du fichier Excel de sortie.
    """
    # Liste pour stocker les lignes de données
    rows = []

    def process_table(table, dependency_path, visited):
        """
        Récursivement, ajoute les dépendances Hive dans la liste des lignes tout en évitant les cycles.

        Args:
            table (str): Table principale ou dépendance à traiter.
            dependency_path (list): Chemin des dépendances accumulées.
            visited (set): Ensemble des tables déjà visitées dans cette branche de récursion.
        """
        if table in visited:
            # Cycle détecté, ajouter une indication de cycle et arrêter cette branche
            current_row = dependency_path + [f"{table} (cycle détecté)"]
            rows.append(current_row)
            return

        # Marquer cette table comme visitée
        visited.add(table)

        # Crée une nouvelle ligne avec la dépendance actuelle
        current_row = dependency_path + [table]
        rows.append(current_row)

        # Continuer avec les dépendances si elles existent
        if table in dependency_map:
            for dep in dependency_map[table]:
                process_table(dep, current_row, visited.copy())  # Passer une copie pour chaque branche

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
                    process_table(hive_table, [rdms_table], set())  # Initialiser "visited" comme un ensemble vide

    # Déterminer le nombre maximum de colonnes pour formater correctement le fichier Excel
    max_columns = max(len(row) for row in rows)
    columns = ["Table_RDMS", "Table_Hive"] + [f"Dep_datalake{i+1}" for i in range(max_columns - 2)]

    # Créer un DataFrame avec les données collectées
    df = pd.DataFrame(rows, columns=columns)

    # Exporter le DataFrame vers un fichier Excel
    df.to_excel(output_file, index=False)
    print(f"Fichier Excel généré avec succès : {output_file}")




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


    



def compare_and_update(reference_file, verification_file, output_file):
    """
    Corrige un fichier Excel en se basant sur un fichier de référence :
    - Seules les tables RDMS présentes dans le fichier à vérifier sont prises en compte.
    - Les lignes du fichier à vérifier sont corrigées en fonction du fichier de référence.
    - Les lignes corrigées ou ajoutées sont annotées dans les commentaires.

    Args:
        reference_file (str): Chemin du fichier de référence généré automatiquement.
        verification_file (str): Chemin du fichier à vérifier.
        output_file (str): Chemin du fichier Excel de sortie.
    """
    # Charger les fichiers Excel
    df_ref = pd.read_excel(reference_file)
    df_verif = pd.read_excel(verification_file)

    # Convertir les colonnes en chaînes pour éviter des erreurs de type
    df_ref = df_ref.astype(str).fillna("")
    df_verif = df_verif.astype(str).fillna("")

    # Initialiser le DataFrame pour la sortie
    corrected_rows = []

    # Filtrer les tables RDMS présentes dans le fichier à vérifier
    rdms_tables = df_verif["Table_RDMS"].unique()

    # Vérifier et corriger ligne par ligne
    for table in rdms_tables:
        # Extraire les lignes pour cette table dans les deux fichiers
        ref_table = df_ref[df_ref["Table_RDMS"] == table]
        verif_table = df_verif[df_verif["Table_RDMS"] == table]

        for _, verif_row in verif_table.iterrows():
            if not ((ref_table == verif_row).all(axis=1)).any():
                # Ligne présente dans le fichier à vérifier mais incorrecte ou incomplète
                matched_ref = ref_table[(ref_table["Table_RDMS"] == verif_row["Table_RDMS"]) & 
                                        (ref_table["Table_Hive"] == verif_row["Table_Hive"])]
                if not matched_ref.empty:
                    # Compléter la ligne à partir de la référence
                    corrected_row = matched_ref.iloc[0].tolist()
                    if corrected_row not in corrected_rows:  # Ajouter seulement si unique
                        corrected_rows.append(corrected_row + ["Corrigée (manquante ou incorrecte)"])
                else:
                    # Ligne totalement absente de la référence
                    corrected_row = verif_row.tolist()
                    if corrected_row not in corrected_rows:  # Ajouter seulement si unique
                        corrected_rows.append(corrected_row + ["Non trouvée dans la référence"])
            else:
                # Ligne correcte
                corrected_row = verif_row.tolist()
                if corrected_row not in corrected_rows:  # Ajouter seulement si unique
                    corrected_rows.append(corrected_row + ["OK"])

    # Ajouter les colonnes de commentaires au fichier final
    columns = list(df_verif.columns) + ["Commentaires"]
    final_df = pd.DataFrame(corrected_rows, columns=columns)

    # Sauvegarder dans le fichier Excel
    final_df.to_excel(output_file, index=False, engine="openpyxl")
    wb = load_workbook(output_file)
    ws = wb.active

    # Appliquer des styles pour la mise en évidence
    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")  # Vert clair
    red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")  # Rouge clair

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        comment_cell = row[-1]  # Dernière colonne (Commentaires)
        if comment_cell.value == "Corrigée (manquante ou incorrecte)":
            for cell in row:
                cell.fill = green_fill
        elif comment_cell.value == "Non trouvée dans la référence":
            for cell in row:
                cell.fill = red_fill

    # Enregistrer le fichier avec la mise en forme
    wb.save(output_file)
    print(f"Fichier corrigé généré avec succès : {output_file}")


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
