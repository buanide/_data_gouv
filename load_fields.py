import json

with open("tables_mon_fields_description_dict.json", "r", encoding="utf-8") as json_file:
    table_dict = json.load(json_file)

name='MON.FT_A_CNI_EXPIREES'


for key, value in table_dict.items():
    if key == name:
        print(key, value)
        break