import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, Date, Time, Boolean
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime


CSV_FILE_PATH = "Traffic_Violations_PROCESSED.csv"
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print(f"CSV size: {df.shape}")
print(df.head())


def convert_to_boolean(value):
    return True if value == 'Yes' else False if value == 'No' else value

def convert_all_booleans(df):
    for column in df.columns:
        if df[column].dtype == 'object':
            unique_values = df[column].dropna().unique()
            if set(unique_values).issubset({'Yes', 'No'}):
                df[column] = df[column].apply(convert_to_boolean)
    return df

df = convert_all_booleans(df)
df['date_of_stop'] = pd.to_datetime(df['date_of_stop'], errors='coerce')


df['traffic_stop_id'] = np.arange(1, len(df) + 1)


Base = declarative_base()

class TrafficStop(Base):
    __tablename__ = 'traffic_stop'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date_of_stop = Column(Date, nullable=False)
    time_of_stop = Column(Time, nullable=False)
    agency = Column(String(100), nullable=False)
    subagency = Column(String(100), nullable=False)
    description = Column(String(255), nullable=True)
    location = Column(String(255), nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    geolocation = Column(String(255), nullable=True)
    state = Column(String(255), nullable=False)

class IncidentDetails(Base):
    __tablename__ = 'incident_details'
    id = Column(Integer, primary_key=True, autoincrement=True)
    traffic_stop_id = Column(Integer, ForeignKey('traffic_stop.id'), nullable=False)
    accident = Column(Boolean, nullable=False)
    belts = Column(Boolean, nullable=False)
    personal_injury = Column(Boolean, nullable=False)
    property_damage = Column(Boolean, nullable=False)
    fatal = Column(Boolean, nullable=False)
    alcohol = Column(Boolean, nullable=False)
    work_zone = Column(Boolean, nullable=False)
    

class Vehicle(Base):
    __tablename__ = 'vehicle'
    id = Column(Integer, primary_key=True, autoincrement=True)
    traffic_stop_id = Column(Integer, ForeignKey('traffic_stop.id'), nullable=False)
    vehicletype = Column(String(50), nullable=False)
    year = Column(Integer, nullable=False)
    make = Column(String(100), nullable=False)
    model = Column(String(100), nullable=False)
    color = Column(String(50), nullable=False)

class Violation(Base):
    __tablename__ = 'violation'
    id = Column(Integer, primary_key=True, autoincrement=True)
    traffic_stop_id = Column(Integer, ForeignKey('traffic_stop.id'), nullable=False)
    violation_type = Column(String(100), nullable=False)
    charge = Column(String(100), nullable=False)
    article = Column(String(100), nullable=False)
    contributed_to_accident = Column(Boolean, nullable=False)

class Driver(Base):
    __tablename__ = 'driver'
    id = Column(Integer, primary_key=True, autoincrement=True)
    traffic_stop_id = Column(Integer, ForeignKey('traffic_stop.id'), nullable=False)
    race = Column(String(50), nullable=False)
    gender = Column(String(10), nullable=False)
    driver_city = Column(String(100), nullable=False)
    driver_state = Column(String(50), nullable=False)
    dl_state = Column(String(50), nullable=False)

class Arrest(Base):
    __tablename__ = 'arrest'
    id = Column(Integer, primary_key=True, autoincrement=True)
    traffic_stop_id = Column(Integer, ForeignKey('traffic_stop.id'), nullable=False)
    arrest_type = Column(String(100), nullable=False)

class CommercialInfo(Base):
    __tablename__ = 'commercial_info'
    id = Column(Integer, primary_key=True, autoincrement=True)
    traffic_stop_id = Column(Integer, ForeignKey('traffic_stop.id'), nullable=False)
    commercial_license = Column(Boolean, nullable=False)
    hazmat = Column(Boolean, nullable=False)
    commercial_vehicle = Column(Boolean, nullable=False)

engine = create_engine('mysql+pymysql://root:root@localhost:3306/dw', echo=False)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


session.bulk_save_objects([TrafficStop(**row) for row in df[['date_of_stop', 'time_of_stop', 'agency', 'subagency', 'description', 'location', 'latitude', 'longitude', 'geolocation', 'state']].to_dict(orient='records')])
session.commit()

session.bulk_save_objects([IncidentDetails(**row) for row in df[['traffic_stop_id', 'accident', 'belts', 'personal_injury', 'property_damage', 'fatal', 'alcohol', 'work_zone']].to_dict(orient='records')])
session.commit()

session.bulk_save_objects([Vehicle(**row) for row in df[['traffic_stop_id', 'vehicletype', 'year', 'make', 'model', 'color']].to_dict(orient='records')])
session.commit()

session.bulk_save_objects([Violation(**row) for row in df[['traffic_stop_id', 'violation_type', 'charge', 'article', 'contributed_to_accident']].to_dict(orient='records')])
session.commit()

session.bulk_save_objects([Driver(**row) for row in df[['traffic_stop_id', 'race', 'gender', 'driver_city', 'driver_state', 'dl_state']].to_dict(orient='records')])
session.commit()

session.bulk_save_objects([Arrest(**row) for row in df[['traffic_stop_id', 'arrest_type']].to_dict(orient='records')])
session.commit()

session.bulk_save_objects([CommercialInfo(**row) for row in df[['traffic_stop_id', 'commercial_license', 'hazmat', 'commercial_vehicle']].to_dict(orient='records')])
session.commit()

print("Data successfully imported!")
