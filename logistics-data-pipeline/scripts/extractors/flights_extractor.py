"""
OpenSky Network API Extractor
Fetches real-time flight data
"""
import logging
from typing import List, Dict, Optional, Tuple
from .base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class FlightsExtractor(BaseExtractor):
    """Extract flight data from OpenSky Network API"""
    
    def __init__(self):
        super().__init__(base_url="https://opensky-network.org/api")
        
    def extract_flights_by_bbox(
        self, 
        bbox: Tuple[float, float, float, float]
    ) -> Optional[List[Dict]]:
        """
        Extract flights within a bounding box
        
        Args:
            bbox: (lon_min, lat_min, lon_max, lat_max)
            
        Returns:
            List of flight data
        """
        params = {
            'lamin': bbox[1],
            'lomin': bbox[0],
            'lamax': bbox[3],
            'lomax': bbox[2]
        }
        
        logger.info(f"Extracting flights for bbox: {bbox}")
        
        data = self.get("states/all", params=params)
        
        if data and 'states' in data:
            flights = data['states']
            logger.info(f"Extracted {len(flights)} flights")
            return flights
        else:
            logger.warning("No flights found or API error")
            return []
    
    def extract_flights_for_airports(self, airports: List[Dict]) -> Dict[str, List]:
        """
        Extract flights for multiple airports
        
        Args:
            airports: List of airport configs with bbox
            
        Returns:
            Dictionary mapping airport codes to flight data
        """
        results = {}
        
        for airport in airports:
            code = airport['code']
            bbox = tuple(airport['bbox'])
            
            flights = self.extract_flights_by_bbox(bbox)
            results[code] = flights if flights else []
            
        return results
