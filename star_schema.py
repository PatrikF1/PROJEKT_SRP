from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, Date, Boolean
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker 

Base = declarative_base()

engine = create_engine('mysql+pymysql://root:root@localhost:3306/star', echo=True)
print("Pokretanje engine-a")
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()
print("Baza uspješno pokrenuta")

# Tablica činjenica
class FactTrafficStop(Base):
    __tablename__ = 'fact_traffic_stop'
    id = Column(Integer, primary_key=True, autoincrement=True)
    traffic_stop_id = Column(Integer, nullable=False)

    driver_id = Column(Integer, ForeignKey('dim_driver.id'))
    vehicle_id = Column(Integer, ForeignKey('dim_vehicle.id'))
    date_id = Column(Integer, ForeignKey('dim_date.id'))
    location_id = Column(Integer, ForeignKey('dim_location.id'))
    arrest_type_id = Column(Integer, ForeignKey('dim_arrest_type.id'))

    num_violations = Column(Integer)
    accident = Column(Boolean)
    alcohol = Column(Boolean)
    personal_injury = Column(Boolean)

# Dimenzije

class DimDriver(Base):
    __tablename__ = 'dim_driver'
    id = Column(Integer, primary_key=True, autoincrement=True)
    race = Column(String(50))
    gender = Column(String(10))
    driver_city = Column(String(100))
    driver_state = Column(String(50))
    dl_state = Column(String(50))
    version = Column(Integer, default=1)  # Sporo mijenjajuce dimenzije

class DimVehicle(Base):
    __tablename__ = 'dim_vehicle'
    id = Column(Integer, primary_key=True, autoincrement=True)
    vehicletype = Column(String(50))
    year = Column(Integer)
    make = Column(String(100))
    model = Column(String(100))
    color = Column(String(50))
    version = Column(Integer, default=1)  # Sporo mijenjajuce dimenzije

class DimDate(Base):
    __tablename__ = 'dim_date'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date_of_stop = Column(Date)
    year = Column(Integer)
    quarter = Column(String(10))
    month = Column(String(10))
    day = Column(Integer)
    weekday = Column(String(10))

    # Hijerarhija: date_of_stop -> month -> quarter -> year

class DimLocation(Base):
    __tablename__ = 'dim_location'
    id = Column(Integer, primary_key=True, autoincrement=True)
    location = Column(String(255))
    state = Column(String(50))
    latitude = Column(Float)
    longitude = Column(Float)

    # Hijerarhija: location -> state

class DimArrestType(Base):  # Degenerirana dimenzija
    __tablename__ = 'dim_arrest_type'
    id = Column(Integer, primary_key=True, autoincrement=True)
    arrest_type = Column(String(100))

Base.metadata.create_all(engine)
print("Uspješno kreirano")
