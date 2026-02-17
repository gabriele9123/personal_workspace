"""
Unit tests for logistics pipeline
"""
import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime


class TestExtractors:
    """Test extraction scripts"""
    
    @patch('requests.get')
    def test_citybikes_extraction(self, mock_get):
        """Test bike data extraction"""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
        
        from extractors.citybikes_extractor import CityBikesExtractor
        
        mock_response = Mock()
        mock_response.json.return_value = {
            'network': {
                'id': 'test-network',
                'name': 'Test Network',
                'location': {'city': 'TestCity', 'country': 'TC'},
                'stations': [
                    {
                        'id': 'station1',
                        'name': 'Station 1',
                        'latitude': 45.0,
                        'longitude': 9.0,
                        'free_bikes': 5,
                        'empty_slots': 10
                    }
                ]
            }
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        extractor = CityBikesExtractor()
        result = extractor.extract_network('test-network')
        
        assert result is not None
        assert result['id'] == 'test-network'
        assert len(result['stations']) == 1


class TestTransformers:
    """Test transformation scripts"""
    
    def test_bikes_transformation(self):
        """Test bike data transformation"""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
        
        from transformers.bikes_transformer import BikesTransformer
        
        raw_data = [{
            'id': 'test-network',
            'name': 'Test Network',
            'location': {'city': 'TestCity', 'country': 'TC'},
            'stations': [
                {
                    'id': 'station1',
                    'name': 'Station 1',
                    'latitude': 45.0,
                    'longitude': 9.0,
                    'free_bikes': 5,
                    'empty_slots': 10
                }
            ]
        }]
        
        transformer = BikesTransformer()
        df = transformer.transform(raw_data)
        
        assert not df.empty
        assert len(df) == 1
        assert 'station_id' in df.columns
        assert df.iloc[0]['free_bikes'] == 5
        assert df.iloc[0]['total_slots'] == 15


class TestLoaders:
    """Test loading scripts"""
    
    def test_database_creation(self):
        """Test database table creation"""
        import sys
        import os
        import tempfile
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
        
        from loaders.database_schema import DatabaseManager
        
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            manager = DatabaseManager(db_path)
            manager.connect()
            manager.create_tables()
            
            from sqlalchemy import inspect
            inspector = inspect(manager.engine)
            tables = inspector.get_table_names()
            
            assert 'bike_stations' in tables
            assert 'flights' in tables
            
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)
