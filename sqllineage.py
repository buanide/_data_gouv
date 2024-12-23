from sqllineage.runner import LineageRunner
sql = "insert into db1.table11 select * from db2.table21 union select * from db2.table22;"
sql += "insert into db3.table3 select * from db1.table11 join db1.table12;"
result = LineageRunner(sql)
# To show lineage summary


# To parse all the source tables
for tbl in result.source_tables: print(tbl)

# likewise for target tables
for tbl in result.target_tables: print(tbl)

# To pop up a webserver for visualization
result.draw()