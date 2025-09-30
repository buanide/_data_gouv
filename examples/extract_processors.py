import sqlglot
import os
from data_lineage.format_json import read_json
from data_lineage.data_sources import data_sources_lineage
from data_lineage.fields import export_tracking_lineage_to_excel
from data_lineage.format_json import create_tab_processors
from data_lineage.format_json import to_format_file




flow_file_path=r"C:\Users\ybqb7360\OneDrive - orange.com\Bureau\toolbox\data lineage\_data_gouv\PRODv2.0_format.json"
dic_nifi_flow_file = read_json(flow_file_path)
create_tab_processors(dic_nifi_flow_file)



