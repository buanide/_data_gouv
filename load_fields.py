import json

with open("tables_mon_fields_description_dict.json", "r", encoding="utf-8") as json_file:
    table_dict = json.load(json_file)


for key, value in table_dict.items():
    print(key, value)
    break