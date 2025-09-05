import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import Table, Column, String, MetaData, UniqueConstraint, DateTime, create_engine
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

# Etape 3 : Récupérer les données pour la table dim_directions
df_silver = pd.read_sql(sql_silver, engine_silver)
df_silver["temp_id"] = df_silver["LineRef"] + "-" + df_silver["DirectionRef"] + "-" + df_silver["DestinationName"]
df_silver["id"] = df_silver["temp_id"].apply(lambda x: hashlib.md5(str(x).encode()).hexdigest())
df_gold = df_silver[["id", "DirectionRef", "DestinationName", "DestinationShortName", "Via", "ResponseTimestamp"]]
df_gold = df_gold.drop_duplicates()


# Etape 4 : Chargement des données dans la base de données Gold
metadata = MetaData()

raw_data = Table(
    "dim_directions",
    metadata,
Column("id", String, primary_key=True),
    Column("DirectionRef", String,  nullable=False),
    Column("DestinationName", String, nullable=False),
    Column("DestinationShortName", String, nullable=False),
    Column("Via", String, nullable=True),
    Column("ResponseTimestamp", DateTime, nullable=False),
    UniqueConstraint("id", name="uix_dim_directions"))

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME_3}")
metadata.create_all(engine)

stmt = insert(raw_data).values(df_gold.to_dict(orient="records"))

update_dict = {
    "DirectionRef": stmt.excluded.DirectionRef,
    "DestinationName": stmt.excluded.DestinationName,
    "DestinationShortName": stmt.excluded.DestinationShortName,
    "Via": stmt.excluded.Via,
    "ResponseTimestamp": stmt.excluded.ResponseTimestamp,
}

stmt = stmt.on_conflict_do_update(constraint="uix_dim_directions", set_= update_dict)

with engine.begin() as conn:
    conn.execute(stmt)