"""
Bike Station Data Transformer
Cleans and structures bike-sharing data
"""
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict

logger = logging.getLogger(__name__)


class BikesTransformer:
    """Transform bike station data"""
    
    @staticmethod
    def transform(networks_data: List[Dict]) -> pd.DataFrame:
        """
        Transform raw bike network data to structured format
        
        Args:
            networks_data: List of network data from API
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Transforming bike station data")
        
        all_stations = []
        
        for network in networks_data:
            network_id = network.get('id')
            network_name = network.get('name')
            city = network.get('location', {}).get('city', 'Unknown')
            country = network.get('location', {}).get('country', 'Unknown')
            
            for station in network.get('stations', []):
                all_stations.append({
                    'network_id': network_id,
                    'network_name': network_name,
                    'city': city,
                    'country': country,
                    'station_id': station.get('id'),
                    'station_name': station.get('name'),
                    'latitude': station.get('latitude'),
                    'longitude': station.get('longitude'),
                    'free_bikes': station.get('free_bikes', 0),
                    'empty_slots': station.get('empty_slots', 0),
                    'total_slots': station.get('free_bikes', 0) + station.get('empty_slots', 0),
                    'timestamp': datetime.utcnow(),
                    'extracted_at': datetime.utcnow()
                })
        
        df = pd.DataFrame(all_stations)
        
        if df.empty:
            logger.warning("No bike station data to transform")
            return df
        
        # Data quality checks
        df = df.dropna(subset=['station_id', 'latitude', 'longitude'])
        df['free_bikes'] = df['free_bikes'].fillna(0).astype(int)
        df['empty_slots'] = df['empty_slots'].fillna(0).astype(int)
        
        logger.info(f"Transformed {len(df)} bike stations")
        
        return df
