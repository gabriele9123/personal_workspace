"""
CityBikes API Extractor
Fetches bike-sharing station data
"""
import logging
from typing import List, Dict, Optional
from .base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class CityBikesExtractor(BaseExtractor):
    """Extract bike-sharing data from CityBikes API"""
    
    def __init__(self):
        super().__init__(base_url="https://api.citybik.es/v2")
        
    def extract_network(self, network_id: str) -> Optional[Dict]:
        """
        Extract data for a specific bike network
        
        Args:
            network_id: CityBikes network identifier
            
        Returns:
            Network data including stations
        """
        logger.info(f"Extracting data for network: {network_id}")
        
        data = self.get(f"networks/{network_id}")
        
        if data and 'network' in data:
            logger.info(f"Successfully extracted {len(data['network'].get('stations', []))} stations")
            return data['network']
        else:
            logger.error(f"Failed to extract data for {network_id}")
            return None
    
    def extract_all_networks(self, network_ids: List[str]) -> List[Dict]:
        """
        Extract data for multiple networks
        
        Args:
            network_ids: List of network identifiers
            
        Returns:
            List of network data
        """
        results = []
        
        for network_id in network_ids:
            data = self.extract_network(network_id)
            if data:
                results.append(data)
                
        logger.info(f"Extracted data for {len(results)}/{len(network_ids)} networks")
        return results
