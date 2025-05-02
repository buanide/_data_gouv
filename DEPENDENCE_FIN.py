import pandas as pd
from collections import defaultdict
import re

# Chargement des données
file_path = 'C:/Users/YBQB7360/Downloads/dependencies_with_raw_server_filtered_format.csv'
data = pd.read_csv(file_path, encoding='ISO-8859-1', sep=';', dtype=str)
data = data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# Définition des colonnes de métadonnées serveur qui seront reconnues comme mots-clés
server_metadata_columns = ['Server', 'Raw_Path', 'Flux_Name', 'Nb_processor', 
                           'Nb_disabled_processors', 'Hostname', 'Port', 'username']

# Construction du graphe avec gestion de casse
graph = defaultdict(set)
reverse_graph = defaultdict(set)
table_mapping = {}
all_tables = set()  # Pour stocker toutes les tables existantes

# Dictionnaire pour stocker les métadonnées des tables
table_metadata = defaultdict(lambda: defaultdict(set))

# Dictionnaire pour tracer l'origine des tables
table_origin = defaultdict(set)  # Pour savoir dans quelles colonnes chaque table apparaît

# Analyser les données pour comprendre la structure
for _, row in data.iterrows():
    # Récupère toutes les valeurs non vides de la ligne pour les tables
    tables_by_column = {}
    
    # Collecter les valeurs des colonnes Table_RDMS et Table_Hive en priorité
    if pd.notna(row.get('Table_RDMS', None)) and str(row['Table_RDMS']).strip():
        tables_by_column['Table_RDMS'] = row['Table_RDMS']
    if pd.notna(row.get('Table_Hive', None)) and str(row['Table_Hive']).strip():
        tables_by_column['Table_Hive'] = row['Table_Hive']
    
    # Collecter les valeurs des colonnes Dep_datalake
    dep_columns = [col for col in data.columns if col.startswith('Dep_datalake')]
    for col in dep_columns:
        if pd.notna(row.get(col, None)) and str(row[col]).strip():
            tables_by_column[col] = row[col]
    
    table_values = list(tables_by_column.values())
    
    # Enregistre toutes les tables avec leur origine
    for col_name, value in tables_by_column.items():
        value_lower = value.lower()
        table_mapping[value_lower] = value
        all_tables.add(value_lower)
        
        # Enregistrer la colonne d'origine de cette table
        table_origin[value_lower].add(col_name)
        
        # Stocker les métadonnées de serveur pour cette table
        for col in server_metadata_columns:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                table_metadata[value_lower][col].add(row[col])
    
    # Crée les relations entre toutes les paires de tables
    for i in range(len(table_values)):
        for j in range(i+1, len(table_values)):
            src, dep = table_values[i], table_values[j]
            src_lower, dep_lower = src.lower(), dep.lower()
            graph[src_lower].add(dep_lower)
            reverse_graph[dep_lower].add(src_lower)

def get_original_case(table_lower):
    """Retourne le nom original de la table"""
    return table_mapping.get(table_lower, table_lower.upper())

def display_vertical(title, items):
    """Affiche les éléments verticalement avec un titre"""
    print(f"\n{title}:")
    if not items:
        print("  Aucun élément trouvé")
        return
    
    for item in items:
        print(f"  - {item}")
    print(f"\n  Total: {len(items)} éléments")

def display_metadata(table_name, metadata_type=None):
    """
    Affiche les métadonnées d'une table
    
    Args:
        table_name (str): Nom de la table en minuscules
        metadata_type (str): Type spécifique de métadonnée à afficher, ou None pour tout afficher
    """
    table_original = get_original_case(table_name)
    
    # Vérifier si cette table a des métadonnées
    if not table_metadata.get(table_name) or all(not values for values in table_metadata[table_name].values()):
        print(f"\nAucune métadonnée serveur trouvée pour la table '{table_original}'")
        
        # Afficher des informations de diagnostic
        if table_name in table_origin:
            origins = ", ".join(sorted(table_origin[table_name]))
            print(f"La table a été trouvée dans les colonnes: {origins}")
            print("Cependant, aucune métadonnée n'est associée à cette table dans ces colonnes.")
            
            # Vérifier si la table apparaît comme dépendance
            in_deps = []
            for src, deps in graph.items():
                if table_name in deps:
                    in_deps.append(get_original_case(src))
            
            if in_deps:
                print("\nCette table apparaît comme dépendance des tables suivantes:")
                for src in sorted(in_deps):
                    print(f"  - {src}")
        
        return
    
    metadata = table_metadata[table_name]
    
    if metadata_type and metadata_type in metadata and metadata[metadata_type]:
        values = sorted(metadata[metadata_type])
        display_vertical(f"{metadata_type} de '{table_original}'", values)
    elif metadata_type:
        print(f"\nPas d'information '{metadata_type}' trouvée pour la table '{table_original}'")
        # Afficher les métadonnées disponibles
        available_metadata = [key for key, values in metadata.items() if values]
        if available_metadata:
            print(f"Métadonnées disponibles: {', '.join(available_metadata)}")
    else:
        has_metadata = False
        print(f"\nMétadonnées serveur pour '{table_original}':")
        for key in server_metadata_columns:  # Utiliser l'ordre défini
            if key in metadata and metadata[key]:
                has_metadata = True
                values = sorted(metadata[key])
                print(f"\n{key}:")
                for value in values:
                    print(f"  - {value}")
        
        if not has_metadata:
            print("  Aucune métadonnée disponible")

# Interface améliorée
print("\n" + "="*60)
print("SYSTÈME D'ANALYSE DE DÉPENDANCES - AMÉLIORÉ")
print("="*60)

while True:
    question = input("\nVotre question (ou 'quitter'): ").strip()
    if question.lower() in ('quitter', 'exit', 'bye'):
        print("Au revoir !")
        break
    
    # Détection de la table
    table_match = re.search(r'([a-z0-9_]+\.[a-z0-9_]+)', question, re.I)
    if not table_match:
        print("\nAucune table détectée dans la question.")
        continue
    
    table = table_match.group(1)
    table_lower = table.lower()
    table_original = get_original_case(table_lower)
    
    # Vérification existence table
    if table_lower not in all_tables:
        print(f"\nLa table '{table_original}' n'existe pas dans le système.")
        
        # Suggestions de tables similaires
        similar = [t for t in table_mapping.values() 
                  if table_lower.split('.')[0] in t.lower()]
        if similar:
            print("\nTables similaires existantes :")
            display_vertical("Suggestions", similar[:5])  # 5 suggestions max
        continue
    
    # Vérification supplémentaire pour le cas où la question est seulement un nom de table
    # On vérifie si la question contient uniquement le motif de table et éventuellement des espaces
    table_only_pattern = re.compile(r'^\s*[a-z0-9_]+\.[a-z0-9_]+\s*$', re.I)
    if table_only_pattern.match(question):
        print(f"\nVeuillez poser une question spécifique à propos de la table '{table_original}'.")
        print("Exemples de questions possibles:")
        print("  - Quelles sont les dépendances directes de " + table_original + " ?")
        print("  - Qui utilise la table " + table_original + " ?")
        print("  - Sur quel serveur se trouve " + table_original + " ?")
        continue
    
    # Commande spéciale pour diagnostiquer une table
    if re.search(r'\b(diagnostic|diagnose|analyser|analyser)\b', question, re.I):
        print(f"\nDiagnostic de la table '{table_original}':")
        
        # Origine de la table
        if table_lower in table_origin:
            origins = ", ".join(sorted(table_origin[table_lower]))
            print(f"Origine: Table trouvée dans les colonnes {origins}")
        else:
            print("Origine: Inconnue - La table existe mais son origine n'est pas tracée")
        
        # Métadonnées disponibles
        metadata_keys = []
        if table_lower in table_metadata:
            metadata_keys = [k for k, v in table_metadata[table_lower].items() if v]
        
        if metadata_keys:
            print(f"Métadonnées disponibles: {', '.join(metadata_keys)}")
        else:
            print("Métadonnées: Aucune métadonnée disponible pour cette table")
        
        # Relations
        deps = graph.get(table_lower, set())
        if deps:
            print(f"Dépendances: {len(deps)} tables dépendantes")
        else:
            print("Dépendances: Aucune dépendance")
        
        users = reverse_graph.get(table_lower, set())
        if users:
            print(f"Utilisateurs: {len(users)} tables utilisatrices")
        else:
            print("Utilisateurs: Aucun utilisateur")
        
        continue
    
    # Amélioration de la détection des mots-clés pour les métadonnées
    # Création d'un dictionnaire de correspondance entre les mots-clés et les colonnes
    metadata_keywords = {
        'flux': 'Flux_Name',
        'flow': 'Flux_Name',
        'serveur': 'Server',
        'server': 'Server',
        'machine': 'Server',
        'chemin': 'Raw_Path',
        'path': 'Raw_Path',
        'raw': 'Raw_Path',
        'fichier': 'Raw_Path',
        'processeur': 'Nb_processor',
        'processor': 'Nb_processor',
        'disabled': 'Nb_disabled_processors',
        'désactivé': 'Nb_disabled_processors',
        'port': 'Port',
        'username': 'username',
        'user': 'username',
        'utilisateur': 'username',
        'host': 'Hostname',
        'hostname': 'Hostname',
    }
    
    # Recherche de correspondance de mots-clés
    found_metadata_type = None
    for keyword, metadata_type in metadata_keywords.items():
        if re.search(r'\b' + re.escape(keyword) + r'\b', question, re.I):
            found_metadata_type = metadata_type
            break
    
    if found_metadata_type:
        display_metadata(table_lower, found_metadata_type)
        continue
        
    elif re.search(r'\b(où|stocké|hébergé|métadonnée)\b', question, re.I):
        display_metadata(table_lower)
        continue
    
    # Traitement des questions sur les dépendances
    elif re.search(r'(dépendance|dépend|dépendre).*(directe|immédiat)', question, re.I):
        deps = sorted([get_original_case(d) for d in graph.get(table_lower, set())])
        display_vertical(f"Dépendances DIRECTES de '{table_original}'", deps)
    
    elif re.search(r'(dépendance|dépend|dépendre).*(indirecte|tout)', question, re.I):
        deps = set()
        stack = list(graph.get(table_lower, set()))
        while stack:
            current = stack.pop()
            if current not in deps:
                deps.add(current)
                stack.extend(graph.get(current, set()))
        display_vertical(f"Dépendances INDIRECTES de '{table_original}'", 
                       sorted([get_original_case(d) for d in deps]))
    
    elif re.search(r'(qui utilise|dépendant|utilisateur)', question, re.I):
        users = sorted([get_original_case(u) for u in reverse_graph.get(table_lower, set())])
        display_vertical(f"Utilisateurs de '{table_original}'", users)
    
    else:
        # Par défaut, montrer les dépendances directes et les utilisateurs
        print(f"\nAnalyse complète pour '{table_original}':")
        deps = sorted([get_original_case(d) for d in graph.get(table_lower, set())])
        users = sorted([get_original_case(u) for u in reverse_graph.get(table_lower, set())])
        
        display_vertical("Dépendances directes", deps)
        display_vertical("Utilisateurs directs", users)
