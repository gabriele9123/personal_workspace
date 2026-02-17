"""
Flight Data Transformer
Cleans and structures flight tracking data
"""
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List

logger = logging.getLogger(__name__)


class FlightsTransformer:
    """Transform flight tracking data"""
    
    # OpenSky state vector indices
    ICAO24 = 0
    CALLSIGN = 1
    ORIGIN_COUNTRY = 2
    LONGITUDE = 5
    LATITUDE = 6
    ALTITUDE = 7
    ON_GROUND = 8
    VELOCITY = 9
    HEADING = 10
    
    @staticmethod
    def transform(flights_by_airport: Dict[str, List]) -> pd.DataFrame:
        """
        Transform raw flight data to structured format
        
        Args:
            flights_by_airport: Dictionary mapping airport codes to flight states
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Transforming flight data")
        
        all_flights = []
        
        for airport_code, flights in flights_by_airport.items():
            for flight in flights:
                if not flight:
                    continue
                    
                all_flights.append({
                    'airport_code': airport_code,
                    'icao24': flight[FlightsTransformer.ICAO24],
                    'callsign': str(flight[FlightsTransformer.CALLSIGN]).strip() if flight[FlightsTransformer.CALLSIGN] else None,
                    'origin_country': flight[FlightsTransformer.ORIGIN_COUNTRY],
                    'longitude': flight[FlightsTransformer.LONGITUDE],
                    'latitude': flight[FlightsTransformer.LATITUDE],
                    'altitude': flight[FlightsTransformer.ALTITUDE],
                    'on_ground': flight[FlightsTransformer.ON_GROUND],
                    'velocity': flight[FlightsTransformer.VELOCITY],
                    'heading': flight[FlightsTransformer.HEADING],
                    'timestamp': datetime.utcnow(),
                    'extracted_at': datetime.utcnow()
                })
        
        df = pd.DataFrame(all_flights)
        
        if df.empty:
            logger.warning("No flight data to transform")
            return df
        
        # Data quality checks
        df = df.dropna(subset=['icao24', 'latitude', 'longitude'])
        df['altitude'] = df['altitude'].fillna(0).astype(float)
        df['velocity'] = df['velocity'].fillna(0).astype(float)
        df['on_ground'] = df['on_ground'].fillna(False).astype(bool)
        
        logger.info(f"Transformed {len(df)} flights")
        
        return df
