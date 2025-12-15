import pytest
from unittest.mock import MagicMock, Mock, patch
from src.clean import DataCleaner
import pandas as pd
from datetime import datetime

def test_clean() -> None:
    with patch('src.clean.MongoClient') as MockClient:
        mock_raw_collection = Mock()
        mock_raw_collection.find.return_value = [
            {
                'serial_number': 1,
                'list_year': 2023,
                'date_recorded': '5/15/2023',
                'town': 'Glassboro',
                'address': '123 Main St',
                'assessed_value': '200000',
                'sale_amount': '250000',
                'sales_ratio': 0.8,
                'property_type': 'Residential'
            }
        ]
        
        mock_clean_collection = Mock()
        mock_clean_collection.delete_many.return_value = None
        mock_clean_collection.insert_many.return_value = None
        
        def get_collection(name: str) -> Mock:
            if name == 'real_estate_raw':
                return mock_raw_collection
            else:
                return mock_clean_collection
        
        mock_db = Mock()
        mock_db.__getitem__ = Mock(side_effect=get_collection)
        
        mock_client_instance = Mock()
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        MockClient.return_value = mock_client_instance
        
        cleaner = DataCleaner("mongodb://localhost:27017", "test_db")
        result = cleaner.clean()
        
        assert isinstance(result, int)

def test_remove_duplicates() -> None:
    df = pd.DataFrame({
        'address': ['123 Main St', '123 Main St', '456 Maple Lane'],
        'date_recorded': ['2023-01-01', '2023-01-01', '2023-01-02'] 
    })
    cleaner = DataCleaner.__new__(DataCleaner)
    
    result = cleaner._remove_duplicates(df)
    
    assert len(result) == 2

def test_clean_collection() -> None:
    with patch('src.clean.MongoClient') as MockClient:
        mock_raw_collection = Mock()
        mock_raw_collection.find.return_value = [
            {
                'serial_number': 1,
                'list_year': 2023,
                'date_recorded': '5/15/2023',
                'town': 'Glassboro',
                'address': '123 Main St',
                'assessed_value': '200000',
                'sale_amount': '250000',
                'sales_ratio': 0.8,
                'property_type': 'Residential'
            }
        ]
        
        mock_clean_collection = Mock()
        mock_clean_collection.delete_many.return_value = None
        mock_clean_collection.insert_many.return_value = None

        def get_collection(name: str) -> Mock:
            if name == 'real_estate_raw':
                return mock_raw_collection
            else:
                return mock_clean_collection
        
        mock_db = Mock()
        mock_db.__getitem__ = Mock(side_effect=get_collection)

        mock_client_instance = Mock()
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        MockClient.return_value = mock_client_instance

        cleaner = DataCleaner("mongodb://localhost:27017", "test_db")
        
        #verify
        calls = [call[0][0] for call in mock_db.__getitem__.call_args_list]
        assert 'real_estate_raw' in calls
        assert 'real_estate_clean' in calls