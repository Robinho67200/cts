import requests
import pandas as pd
from sqlalchemy import Table, Column, String, MetaData, UniqueConstraint, DateTime, Boolean, create_engine, Float
from sqlalchemy.dialects.postgresql import insert
import os
from dotenv import load_dotenv

# Etape 1 : Charger les variables d'environnement
load_dotenv()

DB_NAME = os.getenv("DB_NAME_CTS")
USER = os.getenv("USER_CTS")
PASSWORD = os.getenv("PASSWORD_CTS")
HOST = os.getenv("HOST_CTS")
PORT = os.getenv("PORT_CTS")
KEY_API = os.getenv("KEY_API_CTS")

# Etape 2 : Récupérer les données de l'API CTS
response = requests.get("https://api.cts-strasbourg.eu/v1/siri/2.0/stoppoints-discovery", auth=(KEY_API, ''))

data = response.json()

df = pd.json_normalize(data, record_path=[
        "StopPointsDelivery",
        "AnnotatedStopPointRef"],
                       meta = [["StopPointsDelivery", "ResponseTimestamp"]])

df = (
    df
    .rename(columns = {"Location.Longitude": "Longitude", "Location.Latitude": "Latitude", "Extension.StopCode" : "StopCode", "Extension.LogicalStopCode": "LogicalStopCode", "Extension.IsFlexhopStop": "IsFlexhopStop", "StopPointsDelivery.ResponseTimestamp": "ResponseTimestamp"})
)

# Etape 3 : Chargement des données dans la base de données Bronze
metadata = MetaData()

raw_data = Table(
    "raw_data_stations_discovery",
    metadata,
    Column("StopPointRef", String),
    Column("StopName", String),
    Column("Longitude", Float),
    Column("Latitude", Float),
    Column("StopCode", String),
    Column("LogicalStopCode", String),
    Column("IsFlexhopStop", Boolean),
    Column("ResponseTimestamp", DateTime),
    UniqueConstraint("StopPointRef", "StopName", "Longitude", "Latitude", "StopCode", "LogicalStopCode","IsFlexhopStop", "ResponseTimestamp", name="uix_raw_data_stations_discovery"))

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")
metadata.create_all(engine)

stmt = insert(raw_data).values(df.to_dict(orient="records"))

stmt = stmt.on_conflict_do_nothing(constraint="uix_raw_data_stations_discovery")

with engine.begin() as conn:
    conn.execute(stmt)