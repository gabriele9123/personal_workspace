"""
Data Loader
Loads transformed data into database
"""
import logging
import pandas as pd
from .database_schema import DatabaseManager, BikeStation, Flight

logger = logging.getLogger(__name__)


class DataLoader:
    """Load data into database"""
    
    def __init__(self, db_path: str):
        self.db_manager = DatabaseManager(db_path)
        self.db_manager.connect()
        self.db_manager.create_tables()
        
    def load_bikes(self, df: pd.DataFrame) -> int:
        """
        Load bike station data to database
        
        Args:
            df: DataFrame with bike station data
            
        Returns:
            Number of records inserted
        """
        if df.empty:
            logger.warning("Empty bike data DataFrame, skipping load")
            return 0
            
        logger.info(f"Loading {len(df)} bike station records")
        
        try:
            df.to_sql(
                'bike_stations',
                self.db_manager.engine,
                if_exists='append',
                index=False
            )
            
            logger.info(f"Successfully loaded {len(df)} bike records")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load bike data: {e}")
            raise
            
    def load_flights(self, df: pd.DataFrame) -> int:
        """
        Load flight data to database
        
        Args:
            df: DataFrame with flight data
            
        Returns:
            Number of records inserted
        """
        if df.empty:
            logger.warning("Empty flights data DataFrame, skipping load")
            return 0
            
        logger.info(f"Loading {len(df)} flight records")
        
        try:
            df.to_sql(
                'flights',
                self.db_manager.engine,
                if_exists='append',
                index=False
            )
            
            logger.info(f"Successfully loaded {len(df)} flight records")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load flight data: {e}")
            raise
            
    def get_record_counts(self) -> dict:
        """Get count of records in each table"""
        session = self.db_manager.get_session()
        
        try:
            bike_count = session.query(BikeStation).count()
            flight_count = session.query(Flight).count()
            
            return {
                'bike_stations': bike_count,
                'flights': flight_count
            }
        finally:
            session.close()
