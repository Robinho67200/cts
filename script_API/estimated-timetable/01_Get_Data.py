import requests
import pandas as pd
from sqlalchemy import Table, Column, Integer, String, MetaData, UniqueConstraint, DateTime, Boolean, create_engine
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
response = requests.get("https://api.cts-strasbourg.eu/v1/siri/2.0/estimated-timetable", auth=(KEY_API, ''))

data = response.json()

df = pd.json_normalize(data, record_path=[
        "ServiceDelivery",
        "EstimatedTimetableDelivery",
        "EstimatedJourneyVersionFrame",
        "EstimatedVehicleJourney",
        "EstimatedCalls"],
                       meta = [["ServiceDelivery", "EstimatedTimetableDelivery", "EstimatedJourneyVersionFrame", "EstimatedVehicleJourney", "LineRef"],
                               ["ServiceDelivery", "EstimatedTimetableDelivery", "EstimatedJourneyVersionFrame", "EstimatedVehicleJourney", "DirectionRef"],
                               ["ServiceDelivery", "EstimatedTimetableDelivery", "ResponseTimestamp"]])

df = (
    df
    .rename(columns = {"Extension.IsRealTime": "IsRealTime", "Extension.IsCheckOut": "IsCheckOut", "ServiceDelivery.EstimatedTimetableDelivery.EstimatedJourneyVersionFrame.EstimatedVehicleJourney.LineRef" : "LineRef", "ServiceDelivery.EstimatedTimetableDelivery.ResponseTimestamp": "ResponseTimestamp", "ServiceDelivery.EstimatedTimetableDelivery.EstimatedJourneyVersionFrame.EstimatedVehicleJourney.DirectionRef": "DirectionRef"})
)

# Etape 3 : Chargement des données dans la base de données Bronze
metadata = MetaData()

raw_data = Table(
    "raw_data",
    metadata,
Column("id", Integer, primary_key=True, autoincrement=True),
    Column("StopPointRef", String),
    Column("StopPointName", String),
    Column("DestinationName", String),
    Column("DestinationShortName", String),
    Column("Via", String),
    Column("ExpectedDepartureTime", DateTime),
    Column("ExpectedArrivalTime", DateTime),
    Column("IsRealTime", Boolean),
    Column("IsCheckOut", Boolean),
    Column("LineRef", String),
    Column("DirectionRef", String),
    Column("ResponseTimestamp", DateTime),
    UniqueConstraint("StopPointRef","StopPointName","DestinationName","DestinationShortName","ExpectedDepartureTime","ExpectedArrivalTime","IsRealTime","IsCheckOut","LineRef","DirectionRef", name="uix_raw_data"))

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")
metadata.create_all(engine)

stmt = insert(raw_data).values(df.to_dict(orient="records"))
stmt = stmt.on_conflict_do_nothing(constraint="uix_raw_data")

with engine.begin() as conn:
    conn.execute(stmt)