from utils import list_all_files
import re
import sqlparse
from collections import defaultdict

scripts_dir=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\SCRIPTS"
# Exemple d'utilisation
file_path = r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF\SCORES_CBM\DORMANCE_GLOBALE\PREDICTION\create_table_spark_dormance_globale_prediction_monthly.hql"  # Remplacez par le chemin de votre fichier .hql
list_scripts=list_all_files(scripts_dir)


def extract_table_details(file_path):
    """
    Extrait le nom de la table, les noms des champs, les informations de partition 
    et la présence de 'IF NOT EXISTS' d'une requête CREATE TABLE dans un fichier .hql.
    
    :param file_path: Chemin du fichier .hql
    :return: Un tuple contenant le nom de la table, une liste des champs, les informations de partition et si 'IF NOT EXISTS' est utilisé
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Adapter la regex pour prendre en compte 'IF NOT EXISTS', 'PARTITIONED BY' et autres options
        create_table_match = re.search(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\S+)\s*\((.*?)\)\s*(PARTITIONED\s+BY\s*\(.*?\))?\s*STORED\s+AS\s+\w+",
            content,
            re.DOTALL | re.IGNORECASE
        )
        
        if not create_table_match:
            # Vérifier sans la partie 'IF NOT EXISTS' (au cas où elle est absente)
            create_table_match = re.search(
                r"CREATE\s+TABLE\s+(\S+)\s*\((.*?)\)\s*(PARTITIONED\s+BY\s*\(.*?\))?\s*STORED\s+AS\s+\w+",
                content,
                re.DOTALL | re.IGNORECASE
            )
            if not create_table_match:
                print("Aucune requête CREATE TABLE partitionnée avec ou sans IF NOT EXISTS trouvée dans le fichier.")
                return None, [], None, False
        
        # Extraire le nom de la table
        table_name = create_table_match.group(1)
        
        # Extraire le contenu entre parenthèses pour les champs
        table_body = create_table_match.group(2)
        
        # Utiliser une regex pour capturer les noms des champs
        field_names = re.findall(r"\b(\w+)\b\s+\w+", table_body)
        
        # Extraire le champ de partition (s'il existe)
        partitioned_by = create_table_match.group(3)
        
        # Vérifier la présence de 'IF NOT EXISTS'
        if "IF NOT EXISTS" in content:
            if_not_exists = True
        else:
            if_not_exists = False
        
        return table_name, field_names
    
    except FileNotFoundError:
        print(f"Le fichier {file_path} est introuvable.")
        return None, []
    except Exception as e:
        print(f"Erreur : {e}")
        return None, []


def process_hql_files(file_paths):
    """
    Traite une liste de chemins de fichiers HQL pour extraire le nom de la table et ses champs.

    Args:
        file_paths (list): Liste des chemins de fichiers HQL.

    Returns:
        dict: Dictionnaire avec en clé le chemin du fichier HQL et en valeur le nom de la table et une liste de ses champs.
    """
    results = {}

    for file_path in file_paths:
        if file_path.endswith(".hql") and "create" in file_path.lower():
            try:
                table_name, fields_list = extract_table_details(file_path)
                results[file_path] = {
                    "table_name": table_name,
                    "fields": fields_list
                }
            except ValueError as e:
                print(f"Erreur lors du traitement du fichier {file_path}: {e}")
        else:
            print(f"Le fichier {file_path} n'est pas un fichier HQL ou ne contient pas de requête CREATE TABLE.")

    return results


def parse_hql(file_path):
    # Format and split the query

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            hql_query = file.read()

    except ValueError as e:
                print(f"Erreur lors du traitement du fichier {file_path}: {e}")
        
    parsed = sqlparse.format(hql_query, reindent=True, keyword_case='upper')
    statements = sqlparse.parse(parsed)
    lineage = defaultdict(list)

    for statement in statements:
        if statement.get_type() == 'INSERT':
            tokens = [str(token).strip() for token in statement.tokens if not token.is_whitespace]
            target_table = None
            select_clause = None
            
            # Extract the target table
            for i, token in enumerate(tokens):
                if token.upper().startswith("INSERT INTO"):
                    target_table = tokens[i + 1]
            
            # Extract the SELECT clause
            for token in tokens:
                if token.upper().startswith("SELECT"):
                    select_clause = token
                    break
            
            if target_table and select_clause:
                select_lines = select_clause.split("\n")
                for line in select_lines:
                    if " AS " in line:
                        output_col, input_cols = line.split(" AS ")
                        output_col = output_col.strip()
                        input_cols = input_cols.split(",")
                        lineage[output_col].extend([col.strip() for col in input_cols])
    return lineage

#dic_fields=process_hql_files(list_scripts)
path=r"C:\Users\YBQB7360\Downloads\HDFS\HDFS\PROD\CONF\SCORES_CBM\DORMANCE_GLOBALE\PREDICTION\compute_spark_dormance_globale_prediction_monthly.hql"
lineage = parse_hql(path)

for output_col, input_cols in lineage.items():
    print(f"Output Column: {output_col}")
    print(f"Source Columns: {', '.join(input_cols)}")


"""
count=0
for i, dic in dic_fields.items():
    print(f"Nom de la table : {dic['table_name']}")
    print(f"Champs : {dic['fields']}")
    print()
    count+=1
    if count==5:
        break
"""