import pandas as pd

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

    def process_table(table, dependency_path):
        """
        Récursivement, ajoute les dépendances dans le tableau final.
        
        Args:
            table (str): Table principale ou dépendance à traiter.
            dependency_path (list): Chemin hiérarchique des dépendances.
        """
        # Ajouter une ligne pour la table courante et son chemin de dépendance
        row = dependency_path + [table]
        rows.append(row)
        
        # Si la table a des dépendances, continuer récursivement
        if table in dependency_map:
            for dep in dependency_map[table]:
                process_table(dep, row)

    # Parcourir chaque table principale dans le dictionnaire
    for main_table in dependency_map:
        process_table(main_table, [])

    # Trouver le niveau maximum de dépendance pour ajuster les colonnes
    max_depth = max(len(row) for row in rows)
    columns = [f"Dep_datalake{i+1}" for i in range(max_depth)]

    # Créer le DataFrame final
    df = pd.DataFrame(rows, columns=columns)
    df.to_excel(output_file, index=False)
    print(f"Fichier Excel généré : {output_file}")

# Exemple d'utilisation
dependency_map = {
    "MON.SPARK_FT_CLIENT_360": [
        "MON.SPARK_FT_BDI", 
        "MON.SPARK_FT_OG_IC_CALL_SNAPSHOT", 
        "MON.SPARK_FT_CREDIT_TRANSFER", 
        "MON.SPARK_FT_MSC_TRANSACTION", 
        "MON.SPARK_FT_CRA_GPRS", 
        "CDR.SPARK_IT_OM"
    ],
    "MON.SPARK_FT_CREDIT_TRANSFER": [
        "MON.SPARK_FT_CONTRACT_SNAPSHOT", 
        "CDR.SPARK_IT_ZEBRA_TRANSAC"
    ],
    "CDR.SPARK_IT_ZEBRA_TRANSAC": [
        "CDR.SPARK_IT_OMNY_TRANSACTIONS"
    ]
}

output_file = "try.xlsx"
generate_excel(dependency_map, output_file)
