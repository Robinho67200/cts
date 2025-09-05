import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import Table, Column, Integer, String, MetaData, UniqueConstraint, DateTime, Boolean, create_engine
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
FROM raw_data
)
SELECT *
FROM ranking_ingest_date
WHERE "rank" = 1
"""

df_bronze = pd.read_sql(sql_bronze, engine_bronze)

# Etape 3 : Cleaner les données

# 3.1 : Filtrer sur les valeurs avec un checkout égal à True
df_traitement = df_bronze.loc[df_bronze['IsCheckOut'] ==  True]

# 3.2 : Supprimer les doublons inutiles
df_traitement = df_traitement.drop(["rank", "id"], axis= 1)

# 3.3 : Supprimer les doublons
df_traitement = df_traitement.drop_duplicates()

# 3.4 : Supprimer les lignes avec des valeurs nulles pour toutes les colonnes excepté la colonne "Via"
for colonne in df_traitement.columns :
    if colonne != "Via" :
        df_traitement = df_traitement.loc[df_traitement[colonne].isnull() == False]

# Etape 4 : Chargement des données dans la base de données Silver
metadata = MetaData()

raw_data = Table(
    "clean_data",
    metadata,
Column("id", Integer, primary_key=True, autoincrement=True),
    Column("StopPointRef", String, nullable=False),
    Column("StopPointName", String, nullable=False),
    Column("DestinationName", String, nullable=False),
    Column("DestinationShortName", String, nullable=False),
    Column("Via", String, nullable=True),
    Column("ExpectedDepartureTime", DateTime, nullable=False),
    Column("ExpectedArrivalTime", DateTime, nullable=False),
    Column("IsRealTime", Boolean, nullable=False),
    Column("IsCheckOut", Boolean, nullable=False),
    Column("LineRef", String, nullable=False),
    Column("DirectionRef", String, nullable=False),
    Column("ResponseTimestamp", DateTime, nullable=False),
    UniqueConstraint("StopPointRef","StopPointName","DestinationName","DestinationShortName","ExpectedDepartureTime","ExpectedArrivalTime","IsRealTime","IsCheckOut","LineRef","DirectionRef", name="uix_clean_data"))

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME_2}")
metadata.create_all(engine)

stmt = insert(raw_data).values(df_traitement.to_dict(orient="records"))
stmt = stmt.on_conflict_do_nothing(constraint="uix_clean_data")

with engine.begin() as conn:
    conn.execute(stmt)