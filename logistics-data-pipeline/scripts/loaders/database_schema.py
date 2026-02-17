"""
Database Schema Definitions
Defines tables for logistics data
"""
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class BikeStation(Base):
    """Bike station data model"""
    __tablename__ = 'bike_stations'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    network_id = Column(String(100))
    network_name = Column(String(200))
    city = Column(String(100))
    country = Column(String(100))
    station_id = Column(String(100))
    station_name = Column(String(200))
    latitude = Column(Float)
    longitude = Column(Float)
    free_bikes = Column(Integer)
    empty_slots = Column(Integer)
    total_slots = Column(Integer)
    timestamp = Column(DateTime)
    extracted_at = Column(DateTime)


class Flight(Base):
    """Flight tracking data model"""
    __tablename__ = 'flights'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    airport_code = Column(String(10))
    icao24 = Column(String(20))
    callsign = Column(String(20))
    origin_country = Column(String(100))
    longitude = Column(Float)
    latitude = Column(Float)
    altitude = Column(Float)
    on_ground = Column(Boolean)
    velocity = Column(Float)
    heading = Column(Float)
    timestamp = Column(DateTime)
    extracted_at = Column(DateTime)


class DatabaseManager:
    """Manage database connections and operations"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.engine = None
        self.Session = None
        
    def connect(self):
        """Create database connection"""
        logger.info(f"Connecting to database: {self.db_path}")
        self.engine = create_engine(f'sqlite:///{self.db_path}')
        self.Session = sessionmaker(bind=self.engine)
        
    def create_tables(self):
        """Create all tables if they don't exist"""
        logger.info("Creating database tables")
        Base.metadata.create_all(self.engine)
        logger.info("Tables created successfully")
        
    def get_session(self):
        """Get a new database session"""
        if not self.Session:
            self.connect()
        return self.Session()
