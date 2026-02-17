"""
Base extractor class for API calls
Provides retry logic and error handling
"""
import requests
import logging
import time
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class BaseExtractor:
    """Base class for API extractors"""
    
    def __init__(self, base_url: str, timeout: int = 30, max_retries: int = 3):
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        
    def get(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict[str, Any]]:
        """
        Make GET request with retry logic
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response or None if failed
        """
        url = f"{self.base_url}/{endpoint}" if endpoint else self.base_url
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Requesting {url} (attempt {attempt + 1}/{self.max_retries})")
                
                response = requests.get(
                    url,
                    params=params,
                    timeout=self.timeout
                )
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries reached for {url}")
                    return None
