# DATA GOVERNANCE

## Summary

This archive aims to enable the extraction of metadata (table dependencies, field dependencies, field descriptions) from various data sources within the OCM ecosystem:

Dependencies of a data source: an element without which the table could not be calculated.
Field dependencies: field a = field c + field d => field a depends on c and d.

The different Python files are:

utils.py: contains various functions that allow extracting dependencies from the project containing the necessary code for calculating the tables of the data warehouse and data lake.
fields.py: contains various functions to ensure fields lineage
format_json: contains function to parse and structure metadatas from Nifi flow files
dwh.py: contains a scripts that create a dictionary that contains the fields for each table in the datawarehouse
EXTRACTION_CDR.py: scripts to display CDR related to datawarehouse tables

## requirements

You need some elements for this code:
- the hdfs directory
- the dictionary of datawarehouse fields (table_name->list of fields) in .json format
- the nifi flow file in .json format


## tips

Errors explanations: 

- "Aucune table Hive trouvée": means that no variable "flux\.hive\.pre-exec-queries" was found in sqoop***.conf file 

- "Aucune table RDMS trouvées" :means that  "flux\.rdms\.pre-exec-queries\" was found in the sqoop***.conf file


# HOW TO INSTALL THE LIBRARY

1) Install uv :  
- Installation on windows: irm https://astral.sh/uv/install.ps1 | iex

2) Install the library with :

- Locally:
     git clone https://github.com/tonpseudo/ma_librairie.git (you can also download the code and realize the following steps)
     uv venv
     uv pip install -e .


3) modify the demo.py to use function of the package or create your own .py file to use the function that you need

4) to runthe code use: uv run <nom_fichier.py>



# Limitations

- Some fields can't be totally described cause of the complexity of the request
- The order in which caclulation are done in field lineage have to de be refined
- The time of execution for complex Hive Query Language could be improved 



