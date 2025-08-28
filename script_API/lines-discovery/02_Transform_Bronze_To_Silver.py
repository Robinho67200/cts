import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import Table, Column, String, MetaData, UniqueConstraint, DateTime, Boolean, create_engine
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
FROM raw_data_lines_discovery
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

# 3.4 : Supprimer les lignes avec des valeurs nulles pour les colonnes indiqués dans colonnes_check
colonnes_check = ["LineRef", "LineName", "RouteType", "LineHidden", "ResponseTimestamp"]
for colonne in colonnes_check :
        df_traitement = df_traitement.loc[df_traitement[colonne].isnull() == False]

# 3.5 : AJouter des hashtags avant l'identifiant de couleur dans la colonne RouteColor et RouteTextColor
df_traitement['RouteColor'] = df_traitement['RouteColor'].apply(lambda x : f"#{x}")
df_traitement['RouteTextColor'] = df_traitement['RouteTextColor'].apply(lambda x : f"#{x}")


# Etape 4 : Chargement des données dans la base de données Silver
metadata = MetaData()

clean_data = Table(
    "clean_data_lines_discovery",
    metadata,
    Column("LineRef", String, primary_key=True),
    Column("LineName", String, nullable=False),
    Column("RouteType", String, nullable=False),
    Column("RouteColor", String, nullable=True),
    Column("RouteTextColor", String, nullable=True),
    Column("LineHidden", Boolean, nullable=False),
    Column("ResponseTimestamp", DateTime, nullable=False),
    UniqueConstraint("LineRef", name="uix_clean_data_lines_discovery"))

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME_2}")
metadata.create_all(engine)

stmt = insert(clean_data).values(df_traitement.to_dict(orient="records"))

update_dict = {
    "LineName": stmt.excluded.LineName,
    "RouteType": stmt.excluded.RouteType,
    "RouteColor": stmt.excluded.RouteColor,
    "RouteTextColor": stmt.excluded.RouteTextColor,
    "LineHidden": stmt.excluded.LineHidden,
    "ResponseTimestamp": stmt.excluded.ResponseTimestamp,
}

stmt = stmt.on_conflict_do_update(constraint="uix_clean_data_lines_discovery", set_= update_dict)

with engine.begin() as conn:
    conn.execute(stmt)