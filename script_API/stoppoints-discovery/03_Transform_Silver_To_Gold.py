import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import Table, Column, String, MetaData, UniqueConstraint, DateTime, create_engine, Boolean, Float
from sqlalchemy.dialects.postgresql import insert

# Etape 1 : Charger les variables d'environnement
load_dotenv()

DB_NAME = os.getenv("DB_NAME_CTS")
DB_NAME_2 = os.getenv("DB_NAME_CTS_2")
DB_NAME_3 = os.getenv("DB_NAME_CTS_3")
USER = os.getenv("USER_CTS")
PASSWORD = os.getenv("PASSWORD_CTS")
HOST = os.getenv("HOST_CTS")
PORT = os.getenv("PORT_CTS")

# Etape 2 : Connexion à la base de données Silver et récupération des données les plus récentes
engine_silver = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME_2}")

sql_silver = """
WITH ranking_ingest_date AS
(
SELECT *,
DENSE_RANK() OVER(ORDER BY "ResponseTimestamp" DESC) AS rank
FROM clean_data_stations_discovery
)
SELECT *
FROM ranking_ingest_date
WHERE "rank" = 1
"""

# Etape 3 : Récupérer les données pour la table dim_directions
df_silver = pd.read_sql(sql_silver, engine_silver)
df_gold = df_silver.drop("rank", axis= 1)

df_gold = df_gold[["StopCode", "LogicalStopCode", "StopName", "StopPointRef", "Longitude", "Latitude", "IsFlexhopStop", "ResponseTimestamp"]]

# Etape 4 : Chargement des données dans la base de données Gold
metadata = MetaData()


data_dim_stations = Table(
    "dim_stations",
    metadata,
Column("StopCode", String,  primary_key=True),
    Column("LogicalStopCode", String, nullable=False),
    Column("StopName", String, nullable=False),
    Column("StopPointRef", String, nullable=False),
    Column("Longitude", Float, nullable=False),
    Column("Latitude", Float, nullable=False),
    Column("IsFlexhopStop", Boolean, nullable=False),
    Column("ResponseTimestamp", DateTime, nullable=False),
    UniqueConstraint("StopCode", name="uix_dim_stations"))

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME_3}")
metadata.create_all(engine)

stmt = insert(data_dim_stations).values(df_gold.to_dict(orient="records"))

update_dict = {
    "StopPointRef": stmt.excluded.StopPointRef,
    "StopName": stmt.excluded.StopName,
    "Longitude": stmt.excluded.Longitude,
    "Latitude": stmt.excluded.Latitude,
    "LogicalStopCode": stmt.excluded.LogicalStopCode,
    "IsFlexhopStop": stmt.excluded.IsFlexhopStop,
    "ResponseTimestamp": stmt.excluded.ResponseTimestamp,
}

stmt = stmt.on_conflict_do_update(constraint="uix_dim_stations", set_= update_dict)

with engine.begin() as conn:
    conn.execute(stmt)