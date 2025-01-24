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
#output_file=r"C:\Users\YBQB7360\Documents\Data gouvernance\ocm_data_gouv\schema_flow_file_form.json"
#to_format(input_file,output_file)
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
    
def extract_variables(data,search_key):
    """
    permet de rechercher toutes les occurences d'une clé
    """
    variables_list = []
    dic={}

    # Parcours récursif des données
    def recursive_search(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == search_key:
                    variables_list.append(value)  # Ajouter le contenu de "variables"
                recursive_search(value)  # Explorer le sous-dictionnaire
        elif isinstance(obj, list):
            for item in obj:
                recursive_search(item)  # Explorer chaque élément de la liste

    recursive_search(data)
    return variables_list

def extract_dict(data, search_key, search_value):
    """
    Retourne tous les dictionnaires contenant une clé particulière avec une valeur spécifique
    """
    matching_dicts = []

    # Parcours récursif des données
    def recursive_search(obj):
        if isinstance(obj, dict):
            if search_key in obj and obj[search_key] == search_value:
                matching_dicts.append(obj)  # Ajouter le dictionnaire contenant la clé et la valeur
            for key, value in obj.items():
                recursive_search(value)  # Explorer les sous-dictionnaires
        elif isinstance(obj, list):
            for item in obj:
                recursive_search(item)  # Explorer chaque élément de la liste

    recursive_search(data)
    return matching_dicts


key="variables"
variables_content=extract_variables(data_dict,key)
dic_staging_raw={}
staging=None
rep_raw=None

# Vérification du contenu du dictionnaire
def create_scheduled_group_dict(data_dict:dict,search_key:str,search_value:str):
    """
    Permet d'extraire les process groups actifs.

    Args:
        data_dict (dict): Dictionnaire contenant les données à analyser.
        search_key (str): Clé à rechercher dans les dictionnaires.
        search_value (str): Valeur associée à la clé à rechercher.

    Returns:
        tuple: Deux dictionnaires :
            - dic_process_group : {index: [listes des états programmés des processeurs]}
            - dic_status : {index: 'ENABLED' ou 'DISABLED'}
    """
    if data_dict is not None:
        print("Contenu du dictionnaire chargé avec succès.")
        # Exemple : Extraction des dictionnaires contenant la clé 'componentType' avec la valeur 'PROCESS_GROUP'
       
        results = extract_dict(data_dict, search_key, search_value)
        # parcours des process group
        dic_process_group={}
        dc_info_process_group={}
        dic_status={}
        if results:
            for i, result in enumerate(results, start=1):
                #print(f"Dictionnaire #{i} contenant la clé '{search_key}' avec la valeur '{search_value}':")
                if 'componentType' in result.keys():
                    componentType=result.get('componentType',None)
                    #print("componentType",componentType)
                    variables=extract_variables(result,'variables')
                    #print("variables",variables)

                if 'processors' in result.keys():
                    processors=result.get('processors',None)
                    nb_processors=len(processors)
                    nb_scheduled_states=0
                    nb_enable_schedule_state=0
                    list_scheduled_states=[]
                    for p in processors:
                        for process, values in p.items():
                            #print("processs",process)
                            #print("values",values)
                            if 'scheduledState' in p.keys():
                                #print("status",)
                                scheduledState=p.get('scheduledState',None)
                                if scheduledState:
                                    list_scheduled_states.append(scheduledState)
         # verifier les chiffres la prochaine fois
                    nb_enabled=list_scheduled_states.count('ENABLED')
                    nb_disabled=list_scheduled_states.count('DISABLED')
                                #list_scheduled_states.append(scheduledState)
                
                #dic_process_group[i]=list_scheduled_states
                print("nb_disabled",nb_disabled,"nb_processors",nb_processors)
                #dc_info_process_group[i]={'nb_processors':nb_processors,}
            """
            for i,value in dic_process_group.items():
                if 'ENABLED' in value:
                    dic_status[i]='ENABLED'
                else:
                    dic_status[i]='DISABLED'
            """
            #print(json.dumps(result, indent=4, ensure_ascii=False))
            #break
    else:
        print(f"Aucun dictionnaire ne contient la clé '{search_key}' avec la valeur '{search_value}'.")
        #print(json.dumps(result, indent=4, ensure_ascii=False))

    return dic_process_group,dic_status

search_key = "componentType"
search_value = "PROCESS_GROUP"
dic_process_group,dic_status=create_scheduled_group_dict(data_dict,search_key,search_value)

"""
for key in dic_process_group:
    value1 = dic_process_group[key]
    value2 = dic_status[key]
    print(f"Clé: {key} | Valeur dans dict1: {value1} | Valeur dans dict2: {value2}")
"""
#schema = generate_json_schema(data_dict)
#print(schema)

"""
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

    print("rep raw:",rep_raw,"stockage dict",staging)


    for key, value in variables.items():
        print(f"  {key}: {value}")
        if key=='flux.sftp.remote-path':
            staging=value
        
        if key=='flux.hdfs.filedir' or key=='flux.hdfs.raw':
            rep_raw=value

        if rep_raw and staging:
            print("staging_directory:",staging,"raw:",rep_raw)
                #dic_staging_raw[idx]={'staging_directory':staging,'raw':rep_raw}


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
"""
