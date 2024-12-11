import os
import re
import pandas as pd

from collections import defaultdict

def extract_hive_table_and_queries(rdms_table, conf_dir):
    table_name_fragment = rdms_table.split('.')[-1].upper()
    flux_name = f'SQ_EXPORT_SPARK_{table_name_fragment}'
    
    print(f"Le flux name calculé est : {flux_name}")
    hive_table = None
    conf_file_path = None

    print(f"Recherche de fichiers de configuration commençant par 'sqoop-export-spark' et contenant le flux name : {flux_name}")

    for root, dirs, files in os.walk(conf_dir):
        for file in files:
            if file.lower().startswith('sqoop-export-spark') and file.lower().endswith('.conf'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                    flux_name_pattern = re.compile(rf'flux\.name\s*=\s*"{flux_name}"', re.IGNORECASE)
                    rdms_pattern = re.compile(r'flux\.rdms\.pre-exec-queries\s*\+=\s*""".*?FROM\s+(\S+)\s+WHERE', re.IGNORECASE | re.DOTALL)
                    rdms_match = rdms_pattern.search(content)
                    
                    if flux_name_pattern.search(content) or (rdms_match and rdms_match.group(1).lower() == rdms_table.lower()):
                        print(f"Le fichier {file_path} contient le flux name {flux_name} ou la table RDMS : {rdms_table}")
                        
                        hive_pattern = re.compile(r'flux\.hive\.pre-exec-queries\s*\+=\s*""".*?FROM\s+(\S+)\s+WHERE', re.IGNORECASE | re.DOTALL)
                        hive_match = hive_pattern.search(content)
                        if hive_match:
                            hive_table = hive_match.group(1)
                            conf_file_path = file_path
                            print(f"Table Hive trouvée : {hive_table}")
                        else:
                            print(f"Aucune table Hive trouvée dans {file_path}")                
                        
                        return hive_table, file_path
                
    return hive_table, conf_file_path



def extract_data_source_dependencies(conf_dir, search_term=None):
    """
    Parcours les fichiers .conf pour rechercher les dépendances associées à une source de données.
    
    Args:
        conf_dir (str): Le répertoire contenant les fichiers .conf.
        search_term (str): Terme à rechercher pour affiner la recherche.
        
    Returns:
        list: Une liste de tuples (fichier de configuration, dépendances trouvées).
    """
    results = []

    print(f"Recherche des dépendances dans les fichiers .conf de {conf_dir}.")
    if search_term:
        print(f"Terme de recherche spécifié : {search_term}")

    for root, dirs, files in os.walk(conf_dir):
        for file in files:
            if file.lower().endswith('.conf'):  # Filtrer les fichiers .conf uniquement
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                    # Recherche des dépendances potentielles
                    dependencies = []

                    # Recherche dans flux.hive.pre-exec-queries
                    hive_pattern = re.compile(r'flux\.hive\.pre-exec-queries\s*\+=\s*""".*?FROM\s+(\S+)\s+', re.IGNORECASE | re.DOTALL)
                    dependencies += [match.group(1) for match in hive_pattern.finditer(content)]

                    # Recherche dans flux.rdms.pre-exec-queries
                    rdms_pattern = re.compile(r'flux\.rdms\.pre-exec-queries\s*\+=\s*""".*?FROM\s+(\S+)\s+', re.IGNORECASE | re.DOTALL)
                    dependencies += [match.group(1) for match in rdms_pattern.finditer(content)]

                    # Recherche dans flux.pre-exec-queries
                    pre_exec_pattern = re.compile(r'flux\.pre-exec-queries\s*\+=\s*""".*?FROM\s+(\S+)\s+', re.IGNORECASE | re.DOTALL)
                    dependencies += [match.group(1) for match in pre_exec_pattern.finditer(content)]

                    # Recherche dans flux.exec-queries
                    exec_pattern = re.compile(r'flux\.exec-queries\s*\+=\s*""".*?FROM\s+(\S+)\s+', re.IGNORECASE | re.DOTALL)
                    dependencies += [match.group(1) for match in exec_pattern.finditer(content)]

                    # Recherche des fichiers .hql référencés
                    hql_pattern = re.compile(r'flux\.(?:pre|exec)-queries\s*\+=\s*["\'](.+?\.hql)["\']', re.IGNORECASE)
                    hql_files = [match.group(1) for match in hql_pattern.finditer(content)]
                    dependencies += hql_files

                    # Filtrage des dépendances selon le search_term
                    if search_term:
                        filtered_dependencies = [
                            dep for dep in dependencies
                            if re.search(re.escape(search_term), dep, re.IGNORECASE)
                        ]
                        if filtered_dependencies:
                            print(f"Dépendances correspondant au terme '{search_term}' trouvées dans {file_path}: {filtered_dependencies}")
                            results.append((file_path, filtered_dependencies))
                    else:
                        if dependencies:
                            print(f"Dépendances trouvées dans {file_path}: {dependencies}")
                            results.append((file_path, dependencies))

    if not results:
        print("Aucune dépendance trouvée.")
    return results

def parse_hql_file(file_path):
    """
    Analyse un fichier .hql pour extraire les tables mentionnées dans les requêtes.

    Args:
        file_path (str): Chemin vers le fichier .hql.

    Returns:
        list: Liste des tables trouvées dans le fichier.
    """
    tables = []
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        table_pattern = re.compile(r'FROM\s+(\S+)', re.IGNORECASE)
        tables = [match.group(1) for match in table_pattern.finditer(content)]
    return tables

def extract_table_dependencies(conf_dir, search_term=None):
    """
    Parcours les fichiers .conf et .hql pour extraire toutes les dépendances entre tables.

    Args:
        conf_dir (str): Répertoire contenant les fichiers .conf et .hql.
        search_term (str): Terme de recherche pour affiner la recherche initiale.

    Returns:
        dict: Dictionnaire des dépendances entre tables.
    """
    # Étape 1 : Extraire les dépendances initiales des fichiers .conf
    conf_results = extract_data_source_dependencies(conf_dir, search_term)

    # Étape 2 : Préparer une structure pour stocker les dépendances
    dependencies = defaultdict(list)

    for conf_file, tables in conf_results:
        for table in tables:
            if table.endswith('.hql'):
                # Étape 3 : Analyse des fichiers .hql pour leurs propres dépendances
                print("directory:",conf_dir)
                print("table",table)
                
                #hql_path = os.path.join(conf_dir, table)
                hql_path=os.path.join(conf_dir, table.lstrip('/'))
               
                if os.path.exists(hql_path):
                    hql_dependencies = parse_hql_file(hql_path)
                    dependencies[table].extend(hql_dependencies)
                    print(f"Dépendances extraites de {hql_path}: {hql_dependencies}")
                else:
                    print(f"Fichier .hql non trouvé : {hql_path}")
            else:
                # Ajouter les dépendances directes trouvées dans les fichiers .conf
                dependencies[conf_file].append(table)
    # Étape 4 : Générer les chaînes de dépendances
    dependency_chains = []
    visited = set()

    def build_chain(source, chain):
        if source in visited:
            return
        visited.add(source)
        for dep in dependencies[source]:
            dependency_chains.append(f"{source},{dep}")
            build_chain(dep, chain + [dep])

    for source in dependencies:
        build_chain(source, [source])

    return dependency_chains

# Exemple d'appel à la fonction
if __name__ == "__main__":
    conf_dir = 'C:\\Users\\YBQB7360\\Downloads\\HDFS\\HDFS' 
    table = 'MON.FT_A_SUBSCRIPTION'
    
    chains = extract_table_dependencies(conf_dir, search_term=table)
    print("\nChaînes de dépendances:")
    for chain in chains:
        print(chain)


def find_hql_files(hive_table, conf_dir):
    table_name_fragment = hive_table.split('.')[-1].upper()
    flux_name = f'LOAD_{table_name_fragment}'
    
    print(f"Recherche de fichiers de configuration commençant par 'load' et contenant le flux name : {flux_name}")
    
    for root, dirs, files in os.walk(conf_dir):
        for file in files:
            if file.lower().startswith('load')  and file.lower().endswith('.conf'):
                file_path = os.path.join(root, file)
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                    flux_name_pattern = re.compile(rf'flux\.name\s*=\s*"{flux_name}"', re.IGNORECASE)
                    slice_state_query_pattern = re.compile(r'flux\.slice-state-query\s*=\s*"""\s*(.*?)\s*"""', re.IGNORECASE | re.DOTALL)
                    
                    flux_name_match = flux_name_pattern.search(content)
                    slice_state_query_match = slice_state_query_pattern.search(content)
                    
                    # Debugging output
                    #print(f"Vérification du fichier : {file_path}")
                    #print(f"Contenu du fichier : {content}")
                    #print(f"Correspondances pour flux.name : {flux_name_match}")
                    #if slice_state_query_match:
                        #print(f"Contenu extrait de flux.slice-state-query : {slice_state_query_match.group(1)}")
                    
                    # Vérification de la présence exacte de la table Hive dans la chaîne slice-state-query
                    if flux_name_match or (slice_state_query_match and re.search(rf'\b{re.escape(hive_table)}\b', slice_state_query_match.group(1), re.IGNORECASE)):
                        print(f"Le fichier {file_path} contient le flux name {flux_name_match} et la table Hive : {hive_table}")
                        
                        pre_queries_pattern = re.compile(r'flux\.pre-exec-queries\s*\+=\s*"([^"]+)"', re.IGNORECASE)
                        exec_queries_pattern = re.compile(r'flux\.exec-queries\s*\+=\s*"([^"]+)"', re.IGNORECASE)
                            
                        pre_queries = pre_queries_pattern.findall(content)
                        exec_queries = exec_queries_pattern.findall(content)
                            
                        hql_files = {
                            "pre_queries": pre_queries,
                            "exec_queries": exec_queries
                        }
                            
                        return hql_files, file_path
                    
    return None, None

def find_child_tables_in_hql(hql_file_path, parent_table):
    child_tables = set()
    table_pattern = re.compile(r'(FROM|JOIN)\s+([^\s($)]+)', re.IGNORECASE)

    with open(hql_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        matches = table_pattern.findall(content)
        
        for match in matches:
            table_name = match[1]
            if table_name.lower() != parent_table.lower():
                child_tables.add(table_name)

    return list(child_tables)

def find_dimension_and_view_tables(hql_file_path, conf_dir):
    dimension_tables = set()
    view_tables = set()
    table_pattern = re.compile(r'(FROM|JOIN)\s+([^\s($)]+)', re.IGNORECASE)

    with open(hql_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        matches = table_pattern.findall(content)
        
        for match in matches:
            table_name = match[1].upper()
            if "DIM." in table_name:
                dimension_tables.add(table_name)
            elif "VW_" in table_name:
                view_tables.add(table_name)
                view_hql_file_path = find_view_hql_file(table_name, conf_dir)
                if view_hql_file_path:
                    dim_tables_in_view = find_dim_tables_in_view(view_hql_file_path)
                    dimension_tables.update(dim_tables_in_view)

    return list(dimension_tables), list(view_tables)

def find_view_hql_file(view_table, conf_dir):
    view_name_fragment = view_table.split('.')[-1]
    for root, dirs, files in os.walk(conf_dir):
        for file in files:
            if file.lower() == f'create_{view_name_fragment.lower()}.hql':
                return os.path.join(root, file)
    return None

def find_dim_tables_in_view(view_hql_file_path):
    dim_tables = set()
    table_pattern = re.compile(r'(FROM|JOIN)\s+([^\s($)]+)', re.IGNORECASE)

    with open(view_hql_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        matches = table_pattern.findall(content)
        
        for match in matches:
            table_name = match[1]
            if "DIM." in table_name.upper():
                dim_tables.add(table_name)

    return list(dim_tables)

def process_child_tables(rdms_table, child_tables, conf_dir, level=1, parent_table=None):
    results = []
    
    for child_table in child_tables:
        if "SPARK_IT" in child_table.upper():
            continue  # Skip SPARK_IT tables as they contain raw data
        
        hql_files, conf_file_path = find_hql_files(child_table, conf_dir)
        
        child_tables_next_level = set()
        dimension_tables = set()
        view_tables = set()
        pre_queries = []
        exec_queries = []

        if hql_files:
            for pre_query in hql_files.get('pre_queries', []):
                pre_query_path = os.path.join(conf_dir, pre_query.lstrip('/'))
                pre_query_path = pre_query_path.replace('\\', '/')
                pre_queries.append(pre_query_path)
                child_tables_next_level.update(find_child_tables_in_hql(pre_query_path, child_table))

            for exec_query in hql_files.get('exec_queries', []):
                exec_query_path = os.path.join(conf_dir, exec_query.lstrip('/'))
                exec_query_path = exec_query_path.replace('\\', '/')
                exec_queries.append(exec_query_path)
                dims, views = find_dimension_and_view_tables(exec_query_path, conf_dir)
                dimension_tables.update(dims)
                view_tables.update(views)

        table_identifier = f"{child_table}_L{level}"
        if parent_table:
            table_identifier = f"{parent_table}.{table_identifier}"

        results.append({
            "rdms_table": rdms_table,
            "hive_table": table_identifier,
            "pre_queries": pre_queries,
            "exec_queries": exec_queries,
            "child_tables": list(child_tables_next_level),
            "view_tables": list(view_tables),
            "dimension_tables": list(dimension_tables)
        })

        if child_tables_next_level:
            results.extend(process_child_tables(rdms_table, child_tables_next_level, conf_dir, level + 1, table_identifier))

    return results

# Utilisation
# Remplacez par le chemin du dossier 'conf'


#dependencies=extract_data_source_dependencies(conf_dir,table)

"""
hive_table, conf_file_path = extract_hive_table_and_queries(rdms_table, conf_dir)

if hive_table:
    hql_files, conf_file_path = find_hql_files(hive_table, conf_dir)
    
    pre_queries = []
    exec_queries = []
    child_tables = set()
    dimension_tables = set()
    view_tables = set()

    if hql_files:
        for pre_query in hql_files.get('pre_queries', []):
            pre_query_path = os.path.join(conf_dir, pre_query.lstrip('/'))
            pre_query_path = pre_query_path.replace('\\', '/')
            pre_queries.append(pre_query_path)
            child_tables.update(find_child_tables_in_hql(pre_query_path, hive_table))

        for exec_query in hql_files.get('exec_queries', []):
            exec_query_path = os.path.join(conf_dir, exec_query.lstrip('/'))
            exec_query_path = exec_query_path.replace('\\', '/')
            exec_queries.append(exec_query_path)
            dims, views = find_dimension_and_view_tables(exec_query_path, conf_dir)
            dimension_tables.update(dims)
            view_tables.update(views)
    
    initial_result = {
        "rdms_table": rdms_table,
        "hive_table": hive_table,
        "conf_file_path": conf_file_path,
        "pre_queries": pre_queries,
        "exec_queries": exec_queries,
        "child_tables": list(child_tables),
        "view_tables": list(view_tables),
        "dimension_tables": list(dimension_tables)
    }

    all_results = [initial_result]

    if child_tables:
        all_results.extend(process_child_tables(rdms_table, child_tables, conf_dir, 1, hive_table))

    df = pd.DataFrame(all_results)
    df.to_excel('output.xlsx', index=False)
    print("Traitement terminé. Les résultats ont été enregistrés dans 'output_summary.xlsx'.")
else:
    print(f"Table Hive non trouvée pour {rdms_table}")
"""
