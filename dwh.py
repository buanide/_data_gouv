
import oracledb
import pandas as pd
import json
import time


table_dict = {}
connection = oracledb.connect(
    user="mon", 
    password="Mon123ocm#", 
    dsn="172.26.75.14:20303/OCMDWH"
)

# Créer la connexion SQLAlchemy
cursor = connection.cursor()

# Charger la liste des tables (requête dépendant du SGBD)
a = input("Write OK if you want to update table information: ")

query = """
SELECT OWNER, TABLE_NAME, COLUMN_NAME
FROM ALL_TAB_COLUMNS
WHERE OWNER = 'MON' 
ORDER BY OWNER, TABLE_NAME, COLUMN_ID
"""
if a.upper() == 'OK':
    
    cursor.execute(query)
    tables = cursor.fetchall()
    df=pd.DataFrame(tables, columns=['OWNER', 'TABLE_NAME', 'COLUMN_NAME'])

else:
    print("chargement des tables depuis le fichier parquet")

    start_time = time.time()
    df=pd.read_parquet('tables_mon_fields_description.parquet')

for _, row in df.iterrows():
    key = f"{row['OWNER']}.{row['TABLE_NAME']}"  
    value = row['COLUMN_NAME'] 
    if key not in table_dict:
        table_dict[key] = []  

    table_dict[key].append(value)  


with open("tables_mon_fields_description_dict.json", "w", encoding="utf-8") as json_file:
    json.dump(table_dict, json_file, indent=4, ensure_ascii=False)

end=time.time()-start_time

print("Temps d'exécution: ", end)

#df.to_parquet('tables_mon_fields_description.parquet', index=False)

