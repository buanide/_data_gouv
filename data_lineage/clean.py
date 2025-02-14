import pandas as pd

def get_strange_conf(dic_files_queries_paths):

    """
    dict: Un dictionnaire où les clés sont les chemins des fichiers de configuration et les valeurs sont des dictionnaires contenant
              les listes de chemins complets pour les requêtes pré-exécution ('pre_exec') et exécution ('exec'), les noms des cdr visés, les répertoirs tt, et répertoirs raw
    """
    list_conf=[]
    for i,value in dic_files_queries_paths.items():
        print("key",i,"value:",value)
        if value['cdr_tt']==None:
            list_conf.append(i)
    df_conf=pd.DataFrame(list_conf)

    df_conf=df_conf.drop_duplicates()
    name_file="strange_conf_files.xlsx"
    #print("taille",len(df_unique))
    df_conf.to_excel(name_file, index=False)