from sqlalchemy import create_engine
import pandas as pd

# Remplace par ton SGBD et tes informations de connexion
DATABASE_TYPE = 'oracle'  # Ex: 'mysql', 'oracle', 'mssql'
USERNAME = 'mon'
PASSWORD = 'Mon123ocm#'
HOST = '172.26.75.14'
PORT = '20303'  # Ex: 1521 (Oracle), 1433 (SQL Server), 3306 (MySQL)
DATABASE = 'OCMDWH'

# Créer la connexion SQLAlchemy
engine = create_engine(f"{DATABASE_TYPE}://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

# Charger la liste des tables (requête dépendant du SGBD)
with engine.connect() as connection:
    if DATABASE_TYPE == "postgresql":
        query = "SELECT tablename FROM pg_tables WHERE schemaname = 'public';"
    elif DATABASE_TYPE == "mysql":
        query = "SHOW TABLES;"
    elif DATABASE_TYPE == "mssql":
        query = "SELECT name FROM sys.tables;"
    elif DATABASE_TYPE == "oracle":
        query = "SELECT table_name FROM user_tables;"
    else:
        raise ValueError("SGBD non supporté.")

    df = pd.read_sql(query, connection)

# Afficher la liste des tables
print(df)
