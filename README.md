# DATA GOUVERANCE

This archive aims to enable the extraction of metadata (table dependencies, field dependencies, field descriptions) from various data sources within the OCM ecosystem:

Dependencies of a data source: an element without which the table could not be calculated.
Field dependencies: field a = field c + field d => field a depends on c and d.

The different Python files are:

utils.py: contains various functions that allow extracting dependencies from the project containing the necessary code for calculating the tables of the data warehouse and data lake.

tryout.py: allow to generate datawarehouse tables depedencies.

Errors explanations: 

- "Aucune table Hive trouvée": means that no variable "flux\.hive\.pre-exec-queries" was found in sqoop***.conf file 

- "Aucune table RDMS trouvées" :means that  "flux\.rdms\.pre-exec-queries\" was found in the sqoop***.conf file








