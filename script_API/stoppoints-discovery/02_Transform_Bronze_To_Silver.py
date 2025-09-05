import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import Table, Column, String, MetaData, UniqueConstraint, DateTime, Boolean, create_engine, Float
from sqlalchemy.dialects.postgresql import insert

# Etape 1 : Charger les variables d'environnement
load_dotenv()

DB_NAME = os.getenv("DB_NAME_CTS")
DB_NAME_2 = os.getenv("DB_NAME_CTS_2")
USER = os.getenv("USER_CTS")
PASSWORD = os.getenv("PASSWORD_CTS")
HOST = os.getenv("HOST_CTS")
PORT = os.getenv("PORT_CTS")

# Etape 2 : Connexion à la base de données bronze et récupération des données les plus récentes
engine_bronze = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")

sql_bronze = """
WITH ranking_ingest_date AS
(
SELECT *,
DENSE_RANK() OVER(ORDER BY "ResponseTimestamp" DESC) AS rank
FROM raw_data_stations_discovery
)
SELECT *
FROM ranking_ingest_date
WHERE "rank" = 1
"""

df_bronze = pd.read_sql(sql_bronze, engine_bronze)

# Etape 3 : Cleaner les données

# 3.2 : Supprimer la colonne rank
df_traitement = df_bronze.drop(["rank"], axis= 1)

# 3.3 : Supprimer les doublons
df_traitement = df_traitement.drop_duplicates()
df_traitement = df_traitement.drop_duplicates(subset=["StopCode"], keep="last")

# 3.4 : Supprimer les lignes avec des valeurs nulles pour toutes les colonnes
for colonne in df_traitement.columns :
        df_traitement = df_traitement.loc[df_traitement[colonne].isnull() == False]

# Etape 4 : Chargement des données dans la base de données Silver
metadata = MetaData()

clean_data = Table(
    "clean_data_stations_discovery",
    metadata,
    Column("StopPointRef", String, nullable=False),
    Column("StopName", String, nullable=False),
    Column("Longitude", Float, nullable=False),
    Column("Latitude", Float, nullable=False),
    Column("StopCode", String,  primary_key=True),
    Column("LogicalStopCode", String, nullable=False),
    Column("IsFlexhopStop", Boolean, nullable=False),
    Column("ResponseTimestamp", DateTime, nullable=False),
    UniqueConstraint("StopCode", name="uix_clean_data_stations_discovery"))

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME_2}")
metadata.create_all(engine)

stmt = insert(clean_data).values(df_traitement.to_dict(orient="records"))

update_dict = {
    "StopPointRef": stmt.excluded.StopPointRef,
    "StopName": stmt.excluded.StopName,
    "Longitude": stmt.excluded.Longitude,
    "Latitude": stmt.excluded.Latitude,
    "LogicalStopCode": stmt.excluded.LogicalStopCode,
    "IsFlexhopStop": stmt.excluded.IsFlexhopStop,
    "ResponseTimestamp": stmt.excluded.ResponseTimestamp,
}

stmt = stmt.on_conflict_do_update(constraint="uix_clean_data_stations_discovery", set_= update_dict)

with engine.begin() as conn:
    conn.execute(stmt)