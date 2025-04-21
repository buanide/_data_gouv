import pandas as pd
from collections import defaultdict
import re

# Chargement des données
file_path = 'C:/Users/YBQB7360/Documents/all_dependencies_with_raw_server_.csv'
data = pd.read_csv(file_path, encoding='ISO-8859-1', sep=';', dtype=str)
data = data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# Construction du graphe avec gestion de casse
graph = defaultdict(set)
table_mapping = {}
all_tables = set()  # Pour stocker toutes les tables existantes

# Analyser les données pour construire le graphe de dépendances
for _, row in data.iterrows():
    # Collecter les valeurs des colonnes Table_RDMS et Table_Hive en priorité
    tables_by_column = {}
    
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
    
    # Enregistrer toutes les tables
    for col_name, value in tables_by_column.items():
        value_lower = value.lower()
        table_mapping[value_lower] = value  # Garder la casse originale
        all_tables.add(value_lower)
    
    # Créer les relations entre toutes les paires de tables
    for i in range(len(table_values)):
        for j in range(i+1, len(table_values)):
            src, dep = table_values[i], table_values[j]
            src_lower, dep_lower = src.lower(), dep.lower()
            graph[src_lower].add(dep_lower)

# Récupérer toutes les tables de la colonne Table_RDMS
rdms_tables = []
for _, row in data.iterrows():
    if pd.notna(row.get('Table_RDMS', None)) and str(row['Table_RDMS']).strip():
        rdms_tables.append(row['Table_RDMS'].strip())

# Supprimer les doublons
rdms_tables = list(set(rdms_tables))

# Créer le DataFrame de sortie avec les dépendances CDR
output_data = []

# Pour chaque table RDMS, trouver ses dépendances CDR
for table in rdms_tables:
    table_lower = table.lower()
    
    # Récupérer les dépendances directes
    deps = graph.get(table_lower, set())
    
    # Filtrer pour ne garder que les dépendances commençant par "CDR."
    cdr_deps = [table_mapping.get(dep, dep.upper()) for dep in deps 
               if dep in table_mapping and table_mapping.get(dep, "").startswith("CDR.")]
    
    # Si la table a des dépendances CDR, les ajouter au résultat
    if cdr_deps:
        for cdr_dep in cdr_deps:
            output_data.append({
                'Table_RDMS': table,
                'CDR': cdr_dep
            })
    else:
        # Si aucune dépendance CDR, ajouter quand même la table avec une valeur vide
        output_data.append({
            'Table_RDMS': table,
            'CDR': ""
        })

# Convertir en DataFrame
output_df = pd.DataFrame(output_data)

# Définir le chemin du fichier de sortie
output_file_path = 'C:/Users/YBQB7360/Documents/CDR_EXCTRACTION.csv'

# Exporter le DataFrame en CSV
output_df.to_csv(output_file_path, sep=';', index=False, encoding='ISO-8859-1')

print(f"Fichier CSV généré avec succès: {output_file_path}")
print(f"Nombre total de tables RDMS: {len(rdms_tables)}")
print(f"Nombre total de paires Table_RDMS-CDR: {len(output_data)}")