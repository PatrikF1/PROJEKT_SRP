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






Base = declarative_base()

class Vehicle(Base):
    __tablename__ = 'vehicle'
    id = Column(Integer, primary_key=True, autoincrement=True)
    vehicletype = Column(String(50), nullable=False)
    year = Column(Integer, nullable=False)
    make = Column(String(100), nullable=False)
    model = Column(String(100), nullable=False)
    color = Column(String(50), nullable=False)

    traffic_stops = relationship('TrafficStop', back_populates='vehicle')


class Driver(Base):
    __tablename__ = 'driver'
    id = Column(Integer, primary_key=True, autoincrement=True)
    race = Column(String(50), nullable=False)
    gender = Column(String(10), nullable=False)
    driver_city = Column(String(100), nullable=False)
    driver_state = Column(String(50), nullable=False)
    dl_state = Column(String(50), nullable=False)

    traffic_stops = relationship('TrafficStop', back_populates='driver')


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
    arrest_type = Column(String(100), nullable=True)

    driver_id = Column(Integer, ForeignKey('driver.id'), nullable=False)
    vehicle_id = Column(Integer, ForeignKey('vehicle.id'), nullable=False)

    driver = relationship('Driver', back_populates='traffic_stops')
    vehicle = relationship('Vehicle', back_populates='traffic_stops')

    incidents = relationship('IncidentDetails', back_populates='traffic_stop')
    violations = relationship('Violation', back_populates='traffic_stop')
    commercial_info = relationship('CommercialInfo', back_populates='traffic_stop')


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

    traffic_stop = relationship('TrafficStop', back_populates='incidents')


class Violation(Base):
    __tablename__ = 'violation'
    id = Column(Integer, primary_key=True, autoincrement=True)
    traffic_stop_id = Column(Integer, ForeignKey('traffic_stop.id'), nullable=False)
    violation_type = Column(String(100), nullable=False)
    charge = Column(String(100), nullable=False)
    article = Column(String(100), nullable=False)
    contributed_to_accident = Column(Boolean, nullable=False)

    traffic_stop = relationship('TrafficStop', back_populates='violations')


class CommercialInfo(Base):
    __tablename__ = 'commercial_info'
    id = Column(Integer, primary_key=True, autoincrement=True)
    traffic_stop_id = Column(Integer, ForeignKey('traffic_stop.id'), nullable=False)
    commercial_license = Column(Boolean, nullable=False)
    hazmat = Column(Boolean, nullable=False)
    commercial_vehicle = Column(Boolean, nullable=False)

    traffic_stop = relationship('TrafficStop', back_populates='commercial_info')


engine = create_engine('mysql+pymysql://root:root@localhost:3306/dw', echo=False)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()




vehicle_objects = [Vehicle(**row) for _, row in df[['vehicletype', 'year', 'make', 'model', 'color']].iterrows()]
session.add_all(vehicle_objects)
session.commit()
df['vehicle_id'] = [v.id for v in session.query(Vehicle).all()]


driver_objects = [Driver(**row) for _, row in df[['race', 'gender', 'driver_city', 'driver_state', 'dl_state']].iterrows()]
session.add_all(driver_objects)
session.commit()
df['driver_id'] = [d.id for d in session.query(Driver).all()]



traffic_stop_objects = [TrafficStop(**row) for _, row in df[['date_of_stop', 'time_of_stop', 'agency', 'subagency',
    'description', 'location', 'latitude', 'longitude', 'geolocation', 'state', 'arrest_type',
    'driver_id', 'vehicle_id']].iterrows()]
session.add_all(traffic_stop_objects)
session.commit()
df['traffic_stop_id'] = [ts.id for ts in session.query(TrafficStop).all()]


session.bulk_save_objects([IncidentDetails(**row) for row in df[['traffic_stop_id', 'accident', 'belts', 'personal_injury', 'property_damage', 'fatal', 'alcohol', 'work_zone']].to_dict(orient='records')])
session.commit()

session.bulk_save_objects([Violation(**row) for row in df[['traffic_stop_id', 'violation_type', 'charge', 'article', 'contributed_to_accident']].to_dict(orient='records')])
session.commit()

session.bulk_save_objects([CommercialInfo(**row) for row in df[['traffic_stop_id', 'commercial_license', 'hazmat', 'commercial_vehicle']].to_dict(orient='records')])
session.commit()


print(f"Inserted rows into Driver: {session.query(Driver).count()}")
print(f"Inserted rows into Vehicle: {session.query(Vehicle).count()}")
print(f"Inserted rows into TrafficStop: {session.query(TrafficStop).count()}")


print("Data successfully imported!")
