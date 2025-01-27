import json
from jsonschema import Draft7Validator
import pandas as pd



def create_excel_from_dict(dc_info_process_group, output_file):
    """
    Crée un fichier Excel à partir d'un dictionnaire où chaque ligne est unique par `subdir`.

    Args:
        dc_info_process_group (dict): Dictionnaire contenant les informations.
        output_file (str): Chemin du fichier Excel de sortie.

    Returns:
        None
    """
    # Liste pour stocker les lignes du tableau
    rows = []

    # Parcourir le dictionnaire
    for _, info in dc_info_process_group.items():
        subdirs = info.get("subdir")  # Récupérer subdir

        # Gérer les cas où subdir est absent ou None
        if subdirs==None:
            subdirs = ["Aucun subdir"]  # Ajouter une valeur par défaut

        # Vérifier si subdir est une chaîne contenant plusieurs valeurs séparées par ';'
        if isinstance(subdirs, str):
            if ";" in subdirs:
                subdirs = subdirs.split(";")  # Diviser en une liste par ';'
            else:
                subdirs = [subdirs] 
        # Créer une ligne unique pour chaque subdir
        for subdir in subdirs:
            
            row = {
                'groupIdentifier': info.get('groupIdentifier', ''),
                'nb_processors': info.get('nb_processors', ''),
                'nb_disabled': info.get('nb_disabled', ''),
                'staging': info.get('staging', ''),
                'rep_raw': info.get('rep_raw', ''),
                'subdir': subdir.strip(),  # Nettoyer les espaces éventuels
                'port': info.get('port', ''),
                'flux_name': info.get('flux_name', '')
            }
            rows.append(row)
    
    # Convertir la liste de lignes en DataFrame
    df = pd.DataFrame(rows)

    # Supprimer les doublons basés sur `subdir`
    df = df.drop_duplicates()

    # Exporter vers un fichier Excel
    df.to_excel(output_file, index=False, engine="openpyxl")

    #print(f"Fichier Excel généré avec succès : {output_file}")
    


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


def extract_dict_from_key(data, search_key):
    """
    Retourne tous les dictionnaires contenant une clé particulière avec une valeur spécifique
    """
    matching_dicts = []

    # Parcours récursif des données
    def recursive_search(obj):
        if isinstance(obj, dict):
            if search_key in obj :
                matching_dicts.append(obj)  # Ajouter le dictionnaire contenant la clé et la valeur
            for key, value in obj.items():
                recursive_search(value)  # Explorer les sous-dictionnaires
        elif isinstance(obj, list):
            for item in obj:
                recursive_search(item)  # Explorer chaque élément de la liste

    recursive_search(data)
    return matching_dicts

def get_values_variables(data):
    staging=None
    rep_raw=None
    subdir_names=None
    port=None
    flux_name=None
    for idx, variables in enumerate(data, start=1):
        #print(f"Clé 'variables' #{idx}:")
        if 'flux.sftp.remote-path' in variables.keys():
            staging=variables.get('flux.sftp.remote-path',None)

        elif 'flux.stagging.final' in variables.keys():
            staging=variables.get('flux.stagging.final',None)

        if 'flux.hdfs.filedir' in variables.keys() or 'flux.hdfs.raw' in variables.keys():
            rep_raw=variables.get('flux.hdfs.filedir',None)
            if rep_raw == None:
                rep_raw=variables.get('flux.hdfs.raw',None)
        
        if 'flux.hdfs.subdir-names' in variables.keys():
            subdir_names=variables.get('flux.hdfs.subdir-names',None)

        if 'flux.sftp.port' in variables.keys():
            port=variables.get('flux.sftp.port',None)

        if 'flux.name' in variables.keys():
            flux_name=variables.get('flux.name',None)

    return staging,rep_raw,subdir_names,port,flux_name




def create_dic_identifier(data_dict:dict,key:str):
    """
    Permet de créer un dictionnaire contenant les identifiants des process groups.

    Args:
        data_dict (dict): Dictionnaire contenant les données à analyser.

    Returns:
        dict: Dictionnaire contenant les identifiants des peres des process groups.
    """
    dic_identifier = {}
    key = "processGroups"
    groups = extract_dict_from_key(data_dict, key)
    for i, value in enumerate(groups, start=1):
        if 'identifier' in value.keys():
            identifier = value.get('identifier', None)
            if 'name' in value.keys():
                name = value.get('name', None)
            dic_identifier[i] = {'name': name, 'identifier': identifier}
    return dic_identifier

key="processGroups"
dic_identifier=create_dic_identifier(data_dict,key)
for i,value in dic_identifier.items():
    print("id",i,"name",value)



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

        # Exemple : Extraction des dictionnaires contenant la clé 'componentType' avec la valeur 'PROCESS_GROUP'
       
        results = extract_dict(data_dict, search_key, search_value)
        # parcours des process group
        dic_process_group={}
        dc_info_process_group={}
        dic_status={}
       
        if results:
            for i, result in enumerate(results, start=1):
                #print(f"Dictionnaire #{i} contenant la clé '{search_key}' avec la valeur '{search_value}':")
                nb_processors=0
                groupIdentifier=None
                if 'groupIdentifier' in result.keys():
                    groupIdentifier=result.get('groupIdentifier',None)
                    #print("groupIdentifier",groupIdentifier)

                if 'variables' in result.keys():
                    variables=result.get('variables',None)

                #print("type variables",type(variables))
                if 'componentType' in result.keys():
                    #componentType=result.get('componentType',None)
                    #print("componentType",componentType)
                    variables=extract_variables(result,'variables')
                    #print("variables type",variables)
                    
                    staging,rep_raw,subdir_names,port,flux_name=get_values_variables(variables)
                    #print("serveur",staging,"raw",rep_raw,"subdir",subdir_names,"port",port,"flux_name",flux_name)
                    
                if 'processors' in result.keys():
                    processors=result.get('processors',None)
                    nb_processors+=len(processors)
                    list_scheduled_states=[]
                    pos=0
                    for p in processors:
                        if pos==0:
                            if 'identifier'in p.keys():
                                identifier=p.get('identifier',None)
                                #dic_process_group[identifier]=[]
                        if 'scheduledState' in p.keys():
                            #print("status",)
                            scheduledState=p.get('scheduledState',None)
                            if scheduledState:
                                list_scheduled_states.append(scheduledState)
                        pos+=1
         # verifier les chiffres la prochaine fois
                    nb_enabled=list_scheduled_states.count('ENABLED')
                    nb_disabled=list_scheduled_states.count('DISABLED')
                                #list_scheduled_states.append(scheduledState)
                
                #dic_process_group[i]=list_scheduled_states
                #dc_info_process_group[i]={'nb_processors':nb_processors,}
                #print("nb_enabled",nb_enabled,"nb_disabled",nb_disabled)
                    
                    #print("nb_disabled",nb_disabled,"nb_processors",nb_processors)
                    dc_info_process_group[i]={'groupIdentifier':groupIdentifier,'nb_processors':nb_processors,"nb_disabled":nb_disabled,"staging":staging,"rep_raw":rep_raw,"subdir":subdir_names,"port":port,"flux_name":flux_name}
    
    else:
        print(f"Aucun dictionnaire ne contient la clé '{search_key}' avec la valeur '{search_value}'.")
        #print(json.dumps(result, indent=4, ensure_ascii=False))

    return dc_info_process_group


search_key = "componentType"
search_value = "PROCESS_GROUP"

#dic_process_group=create_scheduled_group_dict(data_dict,search_key,search_value)
#create_excel_from_dict(dic_process_group, output_file=r"C:\Users\YBQB7360\Documents\Data gouvernance\process_group.xlsx")

#for i,value in dic_process_group.items():
 #   print("i",i,"value",value)

#schema = generate_json_schema(data_dict)
#print(schema)

