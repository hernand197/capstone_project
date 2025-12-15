import pytest
from unittest.mock import Mock, MagicMock, patch
from src.raw_data import RawDataLoader
import pandas as pd

def test_raw_data_collection() -> None:
    with patch('src.raw_data.MongoClient') as MockClient:
        mock_collection = Mock()
        mock_db = Mock()
        mock_db.__getitem__ = Mock(return_value=mock_collection)

        mock_client_instance = Mock()
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        MockClient.return_value = mock_client_instance 
        
        loader = RawDataLoader("mongodb://localhost:27017", "test_db")
        
        #check it
        mock_client_instance.__getitem__.assert_called_with("test_db")
        mock_db.__getitem__.assert_called_with('real_estate_raw')


def test_csv_inserts() -> None:
    with patch('src.raw_data.MongoClient') as MockClient:

        mock_insert_result = Mock()
        mock_insert_result.inserted_ids = [1, 2, 3, 4, 5]

        mock_collection = Mock()
        mock_collection.count_documents.return_value = 5
        mock_collection.delete_many.return_value = None
        mock_collection.insert_many.return_value = mock_insert_result

        mock_db = Mock()
        mock_db.__getitem__ = Mock(return_value=mock_collection)
        mock_client_instance = Mock()
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        MockClient.return_value = mock_client_instance

        test_df = pd.DataFrame({
            'Serial Number': [1, 2, 3, 4, 5],
            'Town': ['Glassboro', 'Newark', 'Camden', 'Elizabeth', 'Cape May']
        })

        with patch('pandas.read_csv', return_value=test_df):
            loader = RawDataLoader("mongodb://localhost:27017", "test_db")
            result = loader.load_csv("test.csv")

            mock_collection.insert_many.assert_called_once()

            assert result['row_count'] == 5
            assert result['inserted'] == 5
            assert 'Serial Number' in result['schema']
            assert 'Town' in result['schema']

def test_get_stats() -> None:
    with patch('src.raw_data.MongoClient') as MockClient:
        mock_collection = Mock()
        mock_collection.count_documents.return_value = 10
        mock_find_result = Mock()
        mock_find_result.limit.return_value = []
        mock_collection.find.return_value = mock_find_result
        
        mock_db = Mock()
        mock_db.__getitem__ = Mock(return_value=mock_collection)

        mock_client_instance = Mock()
        mock_client_instance.__getitem__ = Mock(return_value=mock_db)
        MockClient.return_value = mock_client_instance
        
        loader = RawDataLoader("mongodb://localhost:27017", "test_db")
        stats = loader.get_stats()
        
        assert stats['row_count'] == 10
        assert 'sample' in stats