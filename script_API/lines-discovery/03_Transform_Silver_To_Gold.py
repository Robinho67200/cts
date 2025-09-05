import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import Table, Column, String, MetaData, UniqueConstraint, DateTime, create_engine, Boolean
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
FROM clean_data_lines_discovery
)
SELECT *
FROM ranking_ingest_date
WHERE "rank" = 1
"""

# Etape 3 : Récupérer les données pour la table dim_directions
df_silver = pd.read_sql(sql_silver, engine_silver)
df_gold = df_silver.drop("rank", axis= 1)


# Etape 4 : Chargement des données dans la base de données Gold
metadata = MetaData()

data_dim_lignes = Table(
    "dim_lignes",
    metadata,
    Column("LineRef", String, primary_key=True),
    Column("LineName", String, nullable=False),
    Column("RouteType", String, nullable=False),
    Column("RouteColor", String, nullable=True),
    Column("RouteTextColor", String, nullable=True),
    Column("LineHidden", Boolean, nullable=False),
    Column("ResponseTimestamp", DateTime, nullable=False),
    UniqueConstraint("LineRef", name="uix_dim_lignes"))

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME_3}")
metadata.create_all(engine)

stmt = insert(data_dim_lignes).values(df_gold.to_dict(orient="records"))

update_dict = {
    "LineName": stmt.excluded.LineName,
    "RouteType": stmt.excluded.RouteType,
    "RouteColor": stmt.excluded.RouteColor,
    "RouteTextColor": stmt.excluded.RouteTextColor,
    "LineHidden": stmt.excluded.LineHidden,
    "ResponseTimestamp": stmt.excluded.ResponseTimestamp,
}

stmt = stmt.on_conflict_do_update(constraint="uix_dim_lignes", set_= update_dict)

with engine.begin() as conn:
    conn.execute(stmt)