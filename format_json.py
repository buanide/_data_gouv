import json
from jsonschema import Draft7Validator

def to_format(input_file,output_file):
# Chemin du fichier JSON brut

    # Lire et formater le fichier
    with open(input_file, "r", encoding="utf-8") as infile:
        data = json.load(infile)  # Charger le JSON brut

    # Écrire les données formatées dans un nouveau fichier
    with open(output_file, "w", encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=4, ensure_ascii=False)

    print(f"Fichier formaté écrit dans : {output_file}")


# Fonction pour extraire les dictionnaires avec la clé "variables"


# Fonction pour lire un fichier JSON, afficher son contenu et le stocker dans un dictionnaire
def read_json(input_file):
    try:
        # Lecture et chargement du fichier JSON
        with open(input_file, "r", encoding="utf-8") as infile:
            data = json.load(infile)

        # Affichage du contenu du fichier JSON
        # Retourner le contenu du fichier JSON sous forme de dictionnaire
        return data

    except json.JSONDecodeError as e:
        print(f"Erreur lors du chargement du fichier JSON : {e}")
    except FileNotFoundError:
        print(f"Le fichier '{input_file}' est introuvable.")
    except Exception as e:
        print(f"Une erreur est survenue : {e}")
        return None

# Chemin du fichier JSON
input_file = r"C:\Users\YBQB7360\Documents\fichier_formate.json"  

# Lecture du fichier JSON et stockage dans un dictionnaire
data_dict = read_json(input_file)

def generate_json_schema(data):
    # Parcourt récursivement les types de données
    if isinstance(data, dict):
        return {"type": "object", "properties": {k: generate_json_schema(v) for k, v in data.items()}}
    elif isinstance(data, list):
        return {"type": "array", "items": generate_json_schema(data[0]) if data else {}}
    elif isinstance(data, str):
        return {"type": "string"}
    elif isinstance(data, int):
        return {"type": "integer"}
    elif isinstance(data, float):
        return {"type": "number"}
    elif isinstance(data, bool):
        return {"type": "boolean"}
    elif data is None:
        return {"type": "null"}
    else:
        return {}
    
def extract_variables(data):
    variables_list = []

    # Parcours récursif des données
    def recursive_search(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "variables":
                    variables_list.append(value)  # Ajouter le contenu de "variables"
                recursive_search(value)  # Explorer le sous-dictionnaire
        elif isinstance(obj, list):
            for item in obj:
                recursive_search(item)  # Explorer chaque élément de la liste

    recursive_search(data)
    return variables_list

variables_content=extract_variables(data_dict)
dic_staging_raw={}
staging=None
rep_raw=None
for idx, variables in enumerate(variables_content, start=1):
    print(f"Clé 'variables' #{idx}:")

    if 'flux.sftp.remote-path' in variables.keys():
        staging=variables.get('flux.sftp.remote-path',None)

    elif 'flux.stagging.final' in variables.keys():
        staging=variables.get('flux.stagging.final',None)

    if 'flux.hdfs.filedir' in variables.keys() or 'flux.hdfs.raw' in variables.keys():
        rep_raw=variables.get('flux.hdfs.filedir',None)
        if rep_raw == None:
            rep_raw=variables.get('flux.hdfs.raw',None)

    print("rep raw:",rep_raw,"staging",staging)
        
    
    
    """
    for key, value in variables.items():
        print(f"  {key}: {value}")
        if key=='flux.sftp.remote-path':
            staging=value
        
        if key=='flux.hdfs.filedir' or key=='flux.hdfs.raw':
            rep_raw=value

        if rep_raw and staging:
            print("staging_directory:",staging,"raw:",rep_raw)
                #dic_staging_raw[idx]={'staging_directory':staging,'raw':rep_raw}
    """

#print(variables_content[1])
#for i, content in enumerate(variables_content, start=1):
 #   print(f"Contenu de 'variables' #{i}: {json.dumps(content, indent=4)}")


    #schema = generate_json_schema(data_dict)
    #print(schema)

# Vérification du contenu du dictionnaire
#if data_dict is not None:
 #   for i, value in data_dict.items():
  #      print(i)
        
        #if isinstance(value, dict):
         #   if 'processGroups' in value:
          #      print(f"Contenu de 'processGroups' pour '{i}':", value['processGroups'])
            #if isinstance(value['processGroups'],dict):
            #    print('process goup keys')
             #   print(value['processGroups'].keys())
        #print(value.keys())
        #print("key:", i, "value:", value)
        #print(n)
        #if "variables" in value: 
         #   print(value['variables'])
        #elif isinstance(value, dict):
         #   print("no variables")
        #n+=1
        #if n==15:
        #    break



# Extraction des dictionnaires avec la clé "variables"
#dic_identifier=extract_identifiers(input_file)
#print(dic_identifier)
