import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import Table, Column, String, MetaData, UniqueConstraint, DateTime, create_engine, ForeignKey
from sqlalchemy.dialects.postgresql import insert
import hashlib

# Etape 1 : Charger les variables d'environnement
load_dotenv()

DB_NAME = os.getenv("DB_NAME_CTS")
DB_NAME_2 = os.getenv("DB_NAME_CTS_2")
DB_NAME_3 = os.getenv("DB_NAME_CTS_3")
USER = os.getenv("USER_CTS")
PASSWORD = os.getenv("PASSWORD_CTS")
HOST = os.getenv("HOST_CTS")
PORT = os.getenv("PORT_CTS")

# Etape 2 : Connexion à la base de données silver et récupération des données les plus récentes
engine_silver = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME_2}")

sql_silver = """
WITH ranking_ingest_date AS
(
SELECT *,
DENSE_RANK() OVER(ORDER BY "ResponseTimestamp" DESC) AS rank
FROM clean_data_estimated_timetable
)
SELECT *
FROM ranking_ingest_date
WHERE "rank" = 1
"""

df_silver = pd.read_sql(sql_silver, engine_silver)

# 2.1 : Traitement des données
df_silver["Direction"] = df_silver["LineRef"] + "-" + df_silver["DirectionRef"] + "-" + df_silver["DestinationName"]
df_silver["DirectionRef"] = df_silver["Direction"].apply(lambda x: hashlib.md5(str(x).encode()).hexdigest())
df_gold = df_silver[["id", "StopCode", "LineRef", "DirectionRef", "ExpectedDepartureTime", "ExpectedArrivalTime"]]


# 2.2 : Récupération des données des tables de dimensions pour garder uniquement les données ayant une correspondance avec les clefs primaires des tables de dimensions
engine_gold = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME_3}")

sql_stations = """
SELECT *
FROM dim_stations
"""

sql_lignes = """
SELECT *
FROM dim_lignes
"""

df_stations = pd.read_sql(sql_stations, engine_gold)
df_lignes = pd.read_sql(sql_lignes, engine_gold)

df_gold = (
    df_gold
    .merge(df_stations, on="StopCode", how="inner")
    .merge(df_lignes, on="LineRef", how="inner")
    [["id", "StopCode", "LineRef", "DirectionRef", "ExpectedDepartureTime", "ExpectedArrivalTime"]]
)



# Etape 3 : Chargement des données dans la base de données Gold
metadata = MetaData()

dim_stations = Table(
    "dim_stations", metadata,
    Column("StopCode", String, primary_key=True)  # obligatoire pour FK
)

dim_lignes = Table(
    "dim_lignes", metadata,
    Column("LineRef", String, primary_key=True)  # obligatoire pour FK
)

dim_directions = Table(
    "dim_directions", metadata,
    Column("id", String, primary_key=True)  # obligatoire pour FK
)


raw_data = Table(
    "fact_table",
    metadata,
Column("id", String, primary_key=True),
    Column("StopCode", String, ForeignKey("dim_stations.StopCode", name="fk_dim_stations"), nullable=False),
    Column("LineRef", String, ForeignKey("dim_lignes.LineRef", name="fk_dim_lignes"), nullable=False),
    Column("DirectionRef", String, ForeignKey("dim_directions.id", name="fk_dim_directions"), nullable=False),
    Column("ExpectedDepartureTime", DateTime, nullable=False),
    Column("ExpectedArrivalTime", DateTime, nullable=True),
    UniqueConstraint("id", name="uix_fact_table"))

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME_3}")
metadata.create_all(engine)

stmt = insert(raw_data).values(df_gold.to_dict(orient="records"))
stmt = stmt.on_conflict_do_nothing(constraint="uix_fact_table")

with engine.begin() as conn:
    conn.execute(stmt)