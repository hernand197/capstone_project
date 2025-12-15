import pandas as pd
from pymongo import MongoClient
import logging

logger = logging.getLogger(__name__)

class RawDataLoader:
    """Processes / Imports raw datasets into MongoDB"""

    def __init__(self, mongo_uri: str, db_name: str):
        self.client: MongoClient = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db['real_estate_raw']
        logger.info(f"Connected to MongoDB: {db_name}")

    def load_csv(self, filepath: str) -> dict:
        """Loading CSV into MongoDB collection"""
        logger.info(f"Reading csv from {filepath}")
        df = pd.read_csv(filepath)
        records = df.to_dict('records')

        #clearing exisitng data
        self.collection.delete_many({})
        result = self.collection.insert_many(records)

        summary = {
            "row_count": self.collection.count_documents({}),
            "schema": df.columns.tolist(),
            "inserted": len(result.inserted_ids),
        }

        logger.info(f"Loaded {summary['inserted']} records")
        return summary
    
    def get_stats(self, sample_size: int = 5) -> dict:
        """Get stats from raw collection"""
        return {
            "row_count": self.collection.count_documents({}),
            "sample": list(self.collection.find().limit(sample_size)),
        }
    
    def close(self):
        """Close MongoDB conenction"""
        self.client.close()
    





