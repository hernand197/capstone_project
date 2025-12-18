import pandas as pd
from pymongo import MongoClient
from pydantic import ValidationError
from typing import List, Dict, Any
from src.schemas import RealEstateDataClean
import logging

logger = logging.getLogger(__name__)

class DataCleaner:

    def __init__(self, mongo_uri: str, db_name: str):
        self.client: MongoClient = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.raw_collection = self.db['real_estate_raw']
        self.clean_collection = self.db['real_estate_clean']
        logger.info(f"Connected to MongoDB: {db_name}")

    def clean(self) -> int:
        logger.info("Starting transformation pipeline")

        #loading raw data
        df = pd.DataFrame(list(self.raw_collection.find()))
        logger.info(f"Loaded {len(df)} records")

        #remoing id field
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)

        #cleaning
        df = self._format_columns(df)
        df = self._trim_whitespace(df)
        df = self._convert_dates(df)
        df = self._convert_numeric_columns(df)
        df = self._remove_duplicates(df)
        df = self._handle_missing_values(df)

        logger.info(f"After cleaning: {len(df)} records")

        #validating with pydantic
        validated_records = self._validate_records(df)

        #loading to clean collection
        self.clean_collection.delete_many({})
        if validated_records:
            self.clean_collection.insert_many(validated_records)
            logger.info(f"Inserted {len(validated_records)} records into clean collection")
        else:
            logger.warning("No valid recrods to insert")
        #logger.info(f"Loaded {len(validated_records)} valid records")
        return len(validated_records)
    
    def _format_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Appling snake case to column names")
        df.columns = (
            df.columns.astype(str)
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("-", "_")
        )
        return df
    
    def _trim_whitespace(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Stripping whitespace from text fields")
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].str.strip()
        return df
    
    def _convert_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Converting dates")
        df['date_recorded'] = pd.to_datetime(df['date_recorded'], format='%m/%d/%Y', errors='coerce')

        df['date_recorded'] = df['date_recorded'].dt.strftime('%Y-%m-%d')
        return df
    
    def _convert_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Converting data types")
        df['sale_amount'] = pd.to_numeric(df['sale_amount'], errors='coerce')
        df['assessed_value'] = pd.to_numeric(df['assessed_value'], errors='coerce')

        return df
    
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        #Only unique address/date_recorded pairs
        logger.info("Removing duplicates")
        initial_count = len(df)
        df = df.drop_duplicates(subset=['address', 'date_recorded'])
        logger.info(f"Removed {initial_count - len(df)} duplicates")
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Handling missing values")
        missing_values = ["", "na", "n/a", "null", "-", "NaT"]
        df = df.replace(missing_values, pd.NA)
        

        #drop those rows
        initial_count = len(df)
        df = df.dropna(subset=['address', 'date_recorded', 'sale_amount', 'assessed_value'])
        logger.info(f"Removed {initial_count - len(df)} rows")
        
        return df

    def _validate_records(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        logger.info(f"Validating {len(df)} records")
    
        validated = []
        records = df.to_dict('records')  
    
        for record in records:
            try:
                validated_record = RealEstateDataClean(**record)
                validated.append(validated_record.model_dump(mode="json"))
            except ValidationError as e:
                #skip invalid rows
                continue
    
        logger.info(f"Validated {len(validated)}/{len(records)} records")
        return validated

    def get_stats(self) -> dict[str, Any]:
        return {
            "row_count": self.clean_collection.count_documents({}),
            "sample": list(self.clean_collection.find().limit(5)),
        }
    
    def display_cleaned_data(self, limit: int = 5) -> None:
    
        print("\n" + "=" * 50)
        print("CLEANED DATA FROM DATABASE")
        print("=" * 50)
    
        #show total count
        total = self.clean_collection.count_documents({})
        print(f"\nTotal Records in Clean Collection: {total}")
    
        #get records directly
        records = self.clean_collection.find({}, {"_id": 0}).limit(limit)
    
        count = 1
        for record in records:
            print(f"\nRecord {count}:")
            for key, value in record.items():
                print(f"  {key}: {value}")
            print("-" * 80)
            count += 1




    def close(self) -> None:
        self.client.close()

