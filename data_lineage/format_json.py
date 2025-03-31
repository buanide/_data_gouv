import json
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
        if subdirs == None:
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
                "groupIdentifier": info.get("groupIdentifier", ""),
                "nb_processors": info.get("nb_processors", ""),
                "nb_disabled": info.get("nb_disabled", ""),
                "staging": info.get("staging", ""),
                "rep_raw": info.get("rep_raw", ""),
                "subdir": subdir.strip(),  # Nettoyer les espaces éventuels
                "port": info.get("port", ""),
                "flux_name": info.get("flux_name", ""),
            }
            rows.append(row)

    # Convertir la liste de lignes en DataFrame
    df = pd.DataFrame(rows)

    # Supprimer les doublons basés sur `subdir`
    df = df.drop_duplicates()

    # Exporter vers un fichier Excel
    df.to_excel(output_file, index=False, engine="openpyxl")

    # print(f"Fichier Excel généré avec succès : {output_file}")


def to_format(input_file):
    with open(input_file, "r", encoding="utf-8") as infile:
        data = json.load(infile)  # Retourner le dictionnaire directement
    
    return data 

def to_format_file(input_file, output_file):
    """
    Lit un fichier JSON et enregistre son contenu sous forme de JSON formaté avec une indentation de 4 espaces.
    
    Paramètres:
        input_file (str): Chemin du fichier JSON à lire.
        output_file (str): Chemin du fichier où enregistrer le JSON formaté.
    
    Retourne:
        None
    """
    with open(input_file, "r", encoding="utf-8") as infile:
        data = json.load(infile)  # Charger le JSON
    
    with open(output_file, "w", encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=4, ensure_ascii=False)   # Retourner un JSON formaté

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




def generate_json_schema(data):
    # Parcourt récursivement les types de données
    if isinstance(data, dict):
        return {
            "type": "object",
            "properties": {k: generate_json_schema(v) for k, v in data.items()},
        }
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


def extract_variables(data, search_key):
    """
    permet de rechercher toutes les occurences d'une clé
    """
    variables_list = []
    dic = {}

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
            if search_key in obj:
                matching_dicts.append(obj)  # Ajouter le dictionnaire contenant la clé et la valeur
            for key, value in obj.items():
                recursive_search(value)  # Explorer les sous-dictionnaires
        elif isinstance(obj, list):
            for item in obj:
                recursive_search(item)  # Explorer chaque élément de la liste

    recursive_search(data)
    return matching_dicts


def get_values_variables(data):
    """
    data(dict): dictionnaire de variables
    permet de récupérer les valeurs des variables d'un process group

    """
    staging = None
    rep_raw = None
    subdir_names = None
    port = None
    flux_name = None
    ip_adress = None
    username = None
    filename_contents_subdir=None
    filenames_regex=None
    for idx, variables in enumerate(data, start=1):
        # print(f"Clé 'variables' #{idx}:")
        if "flux.sftp.remote-path" in variables.keys():
            staging = variables.get("flux.sftp.remote-path", None)

        elif "flux.stagging.final" in variables.keys():
            staging = variables.get("flux.stagging.final", None)

        if "flux.hdfs.filedir" in variables.keys() or "flux.hdfs.raw" in variables.keys():
            rep_raw = variables.get("flux.hdfs.filedir", None)
            if rep_raw == None:
                rep_raw = variables.get("flux.hdfs.raw", None)

        if "flux.hdfs.subdir-names" in variables.keys():
            subdir_names = variables.get("flux.hdfs.subdir-names", None)

        if "flux.sftp.hostname" in variables.keys():
            ip_adress = variables.get("flux.sftp.hostname", None)

        if "flux.sftp.port" in variables.keys():
            port = variables.get("flux.sftp.port", None)

        if "flux.name" in variables.keys():
            flux_name = variables.get("flux.name", None)

        if "flux.sftp.username" in variables.keys():
            username = variables.get("flux.sftp.username", None)

        if "flux.hdfs.filename-contents-subdir" in variables.keys():
            filename_contents_subdir=variables.get("filename_contents_subdir",None)

        if "flux.filenames-regex" in variables.keys():
            filenames_regex=variables.get("flux.filenames-regex",None)


    return staging, rep_raw, subdir_names, port, flux_name, ip_adress, username,filename_contents_subdir,filenames_regex


def create_dic_identifier(data_dict: dict, key: str):
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
        if "identifier" in value.keys():
            identifier = value.get("identifier", None)
            if "name" in value.keys():
                name = value.get("name", None)
            dic_identifier[i] = {"name": name, "identifier": identifier}
    return dic_identifier


# key="processGroups"
# dic_identifier=create_dic_identifier(data_dict,key)
# for i,value in dic_identifier.items():
#    print("id",i,"name",value)


# Vérification du contenu du dictionnaire
def create_scheduled_group_dict(data_dict: dict, search_key: str, search_value: str):
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
        dic_process_group = {}
        dc_info_process_group = {}
        dic_status = {}
        dic_processors = {}

        if results:
            for i, result in enumerate(results, start=1):
                # print(f"Dictionnaire #{i} contenant la clé '{search_key}' avec la valeur '{search_value}':")
                nb_processors = 0
                groupIdentifier = None
                name = None
                if "groupIdentifier" in result.keys():
                    groupIdentifier = result.get("groupIdentifier", None)
                    # print("groupIdentifier",groupIdentifier)

                if "variables" in result.keys():
                    variables = result.get("variables", None)

                # print("type variables",type(variables))
                if "componentType" in result.keys():
                    # componentType=result.get('componentType',None)
                    # print("componentType",componentType)
                    variables = extract_variables(result, "variables")
                    # print("variables type",variables)
                    staging,rep_raw,subdir_names,port,flux_name,ip_adress,username,filename_contents_subdir,filenames_regex= get_values_variables(variables)
                    #print("serveur",staging,"raw",rep_raw,"subdir",subdir_names,"port",port,"flux_name",flux_name)

                if "processors" in result.keys():
                    processors = result.get("processors", None)
                    nb_processors += len(processors)
                    list_scheduled_states = []
                    list_processors_names = []
                    pos = 0
                    nb_list_disabled = 0
                    for p in processors:
                        if pos == 0:
                            if "identifier" in p.keys():
                                identifier = p.get("identifier", None)
                                # dic_process_group[identifier]=[]

                            if "name" in p.keys():
                                name = p.get("name", None)

                        if "scheduledState" in p.keys():
                            # print("status",)
                            scheduledState = p.get("scheduledState", None)
                            if scheduledState:
                                list_scheduled_states.append(scheduledState)
                                if (
                                    name != None
                                    and name == "List Files"
                                    and scheduledState == "DISABLED"
                                ):
                                    nb_list_disabled += 1

                        dic_processors[pos] = {"processors": name}
                        pos += 1

                    nb_enabled = list_scheduled_states.count("ENABLED")
                    nb_disabled = list_scheduled_states.count("DISABLED")
                    # list_scheduled_states.append(scheduledState)

                    # dic_process_group[i]=list_scheduled_states
                    # dc_info_process_group[i]={'nb_processors':nb_processors,}
                    # print("nb_enabled",nb_enabled,"nb_disabled",nb_disabled)

                    # print("nb_disabled",nb_disabled,"nb_processors",nb_processors)
                    dc_info_process_group[i] = {
                        "groupIdentifier": groupIdentifier,
                        "nb_processors": nb_processors,
                        "nb_disabled": nb_disabled,
                        "staging": staging,
                        "rep_raw": rep_raw,
                        "subdir": subdir_names,
                        "port": port,
                        "flux_name": flux_name,
                        "ip_adress": ip_adress,
                        "username": username,
                        "name": name,
                        "nb_list_disabled": nb_list_disabled,
                        "regex":filenames_regex
                    }

    else:
        print(
            f"Aucun dictionnaire ne contient la clé '{search_key}' avec la valeur '{search_value}'."
        )
        # print(json.dumps(result, indent=4, ensure_ascii=False))

    return dc_info_process_group


# search_key = "componentType"
# search_value = "PROCESS_GROUP"

# dic_process_group=create_scheduled_group_dict(data_dict,search_key,search_value)


def update_dict_depedencies(dic: dict, dic_dependencies: dict):
    for table, vaue in dic_dependencies.items():
        depencies = vaue.get("dependencies")
        if depencies:
            raw = depencies[-1]
            if raw:
                if raw.startswith("/"):
                    servers = []
                    for i in dic:
                        rep_raw = i.get("raw_path")
                        if rep_raw == raw:
                            # print("server",i.get('server'),"flux_name",i.get('flux_name'),"raw",raw)
                            server = i.get("server")
                            if server and server not in depencies:
                                servers.append(server)
                                dic_dependencies[table]["server"] = servers
    return dic_dependencies


def filter_best_records(data_list):
    """
    Filtre les dictionnaires en gardant uniquement ceux avec le moins de nb_list_disabled
    pour chaque combinaison unique de raw_path et flux_name.

    Args:
        data_list (list): Liste de dictionnaires contenant les informations.

    Returns:
        list: Liste filtrée avec seulement les meilleurs enregistrements pour chaque combinaison (raw_path, flux_name).
    """
    # Dictionnaire pour stocker le meilleur enregistrement pour chaque combinaison (raw_path, flux_name)
    best_records = {}

    for record in data_list:
        raw_path = record.get("raw_path")
        flux_name = record.get("flux_name")
        nb_list_disabled = record.get("nb_list_disabled")

        # Créer une clé unique (raw_path, flux_name)
        key = (raw_path, flux_name)

        # Si la clé n'est pas encore dans le dictionnaire ou si ce record a un nb_list_disabled plus faible
        if key not in best_records or nb_list_disabled < best_records[key].get("nb_list_disabled"):
            best_records[key] = record

    # Retourner la liste des meilleurs enregistrements
    return list(best_records.values())



def structure_dic(dic_process_group: dict, dic_dependencies: dict):
    """
    Constructs a list of dictionaries where each record contains:
    - 'id': A unique identifier for each entry
    - 'server': The associated staging server
    - 'nb_processors': Total number of processors
    - 'nb_disabled_processors': Total number of disabled processors
    - 'flux_name': The associated flux name (one per record)

    Args:
        dic_process_group (dict): Process group data with keys 'rep_raw', 'staging', 'subdir'.
        dic_dependencies (dict): Dependency information of data warehouse tables.

    Returns:
        list: A structured list of dictionaries representing staging servers, their fluxes, and processing data.
    """

    structured_data = []
    record_id = 1  # Unique identifier counter

    for i, value in dic_dependencies.items():
        dependencies = value.get("dependencies", [])
        # print("table",i,"dependecies",dependencies)

        if not dependencies:
            continue  # Skip if no dependencies

        last_dependency = dependencies[-1]  # Get the last dependency (potential raw path)
        if last_dependency and last_dependency.startswith("/"):
            raw_path = last_dependency  # Detected raw directory
            # Extract the first 4 parts of the path to get the base raw directory
            tab_raw = raw_path.split("/")
            second_to_last = None
            if len(tab_raw) > 3:
                # Example: "/PROD/RAW/OM"
                # raw_base_path = "/".join(tab_raw[:4]).strip()
                raw_base_path = "/".join(tab_raw[:4]).strip().replace("\u200b", "").replace("\xa0", "").replace("\n", "").replace("\r", "")
                
                # print("raw_base_path",raw_base_path,"type",type(raw_base_path))
                # print(f"Checking raw_base_path: {repr(raw_base_path)}")
                # if "/PROD/RAW/MVAS"==raw_base_path:
                for _, elements in dic_process_group.items():
                    rep_raw = elements.get("rep_raw")
                    staging_server = elements.get("staging")
                    subdir = elements.get("subdir")
                    ip_adress = elements.get("ip_adress")
                    if subdir:
                        tab_subdir = subdir.split(";")
                    flux_name = elements.get("flux_name")  # Now ensures only ONE flux per entry
                    username = elements.get("username")
                    port = elements.get("port")
                    group_identifier = elements.get("groupIdentifier")
                    nb_processors = elements.get("nb_processors", 0)
                    nb_disabled_processors = elements.get("nb_disabled", 0)
                    nb_list_processors_disabled = elements.get("nb_list_disabled")
                    regex=elements.get("regex")

                    if staging_server is None or flux_name is None:
                        continue  # Skip if no staging server or flux name

                    if nb_processors == 0:
                        continue  # Avoid division by zero

                    # Check if the raw path matches the process group's raw path
                    if rep_raw and rep_raw == raw_base_path:
                        second_to_last = tab_raw[
                            -2
                        ]  # Example: "TRANSACTIONS" from "/PROD/RAW/OM/TRANSACTIONS"
                        # print("second_to_last", repr(second_to_last))
                        # print("subdir", repr(tab_subdir))

                        # on verifie que le sous repertoire est bien le meme que le sous repertoire du process group
                        if tab_subdir:
                            if subdir != None and second_to_last == tab_subdir[0] or second_to_last in tab_subdir:
                                # print("MVAS_DATA in process_group")
                                # if raw_path=='/PROD/RAW/MVAS/MVAS_DATA/merged_*':
                                # print('nb_processors',nb_processors,'nb_disabled_processors',nb_disabled_processors)
                                """
                                print("serveur",staging_server,"raw",raw_path,"hostname",ip_adress,"port",port,
"flux_name",
                                    flux_name,
                                    "processors_list_disabled",
                                    nb_list_processors_disabled,
                                )
                                """
                                if (nb_disabled_processors / nb_processors) < 0.8:
                                    

                                    structured_data.append(
                                        {
                                            "id": record_id,
                                            "server": staging_server,
                                            "nb_processors": nb_processors,
                                            "nb_disabled_processors": nb_disabled_processors,
                                            "flux_name": flux_name,
                                            "raw_path": raw_path,
                                            "ip_adress": ip_adress,
                                            "username": username,
                                            "port": port,
                                            "nb_list_disabled":nb_list_processors_disabled,
                                            "regex":regex
                                        }
                                    )
                                    record_id += 1

                    # Case where the raw path is not fully detailed in the raw of the proces group ex: PROD/RAW
                    else:
                        if subdir != None:
                            second_to_last = tab_raw[-2]
                            if second_to_last != None:
                                if second_to_last in tab_subdir or second_to_last == tab_subdir[0]:
                                    # if raw_path=='/PROD/RAW/MVAS/MVAS_DATA/merged_*':
                                    # print('nb_processors',nb_processors,'nb_disabled_processors',nb_disabled_processors)
                                    # print("subdir",subdir,"second_to_last",second_to_last,"rep_raw",rep_raw)
                                    staging_server = elements.get("staging")
                                    flux_name = elements.get("flux_name")
                                    if second_to_last == tab_subdir[0] or second_to_last == tab_subdir:
                                        if (nb_disabled_processors / nb_processors) < 0.8:
                                            structured_data.append(
                                                {
                                                    "id": record_id,
                                                    "server": staging_server,
                                                    "nb_processors": nb_processors,
                                                    "nb_disabled_processors": nb_disabled_processors,
                                                    "flux_name": flux_name,
                                                    "raw_path": raw_path,
                                                    "ip_adress": ip_adress,
                                                    "username": username,
                                                    "port": port,
                                                    "nb_list_disabled":nb_list_processors_disabled,
                                                    "regex":regex
                                                }
                                            )
                                            record_id += 1

    structured_data=filter_best_records(structured_data)

    return structured_data




# create_excel_from_dict(dic_process_group, output_file=r"C:\Users\YBQB7360\Documents\Data gouvernance\process_group.xlsx")

# for i,value in dic_process_group.items():
#   print("i",i,"value",value)


# schema = generate_json_schema(data_dict)
# print(schema)

# to_format(r'C:\Users\YBQB7360\Documents\Data gouvernance\ocm_data_gouv\PRODv2.0.json',r'C:\Users\YBQB7360\Documents\Data gouvernance\ocm_data_gouv\formated_PRODv2.0')
