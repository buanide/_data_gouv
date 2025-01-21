import json

# Chemin du fichier JSON brut
input_file = r'C:\Users\YBQB7360\Documents\Data gouvernance\ocm_data_gouv\NiFi_Flow.bak.ner.20250109.json'
output_file = "fichier_formate.json"

# Lire et formater le fichier
with open(input_file, "r", encoding="utf-8") as infile:
    data = json.load(infile)  # Charger le JSON brut

# Écrire les données formatées dans un nouveau fichier
with open(output_file, "w", encoding="utf-8") as outfile:
    json.dump(data, outfile, indent=4, ensure_ascii=False)

print(f"Fichier formaté écrit dans : {output_file}")
