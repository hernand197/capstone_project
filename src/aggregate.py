from pymongo import MongoClient
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class DataAggregator:
    """
    Gold-layer aggregation using Pandas, then writing results back to MongoDB
    """

    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]

        self.clean_collection = self.db["real_estate_clean"]

        self.yearly_collection = self.db["real_estate_gold_yearly"]
        self.town_collection = self.db["real_estate_gold_town"]
        self.property_collection = self.db["real_estate_gold_property"]

        logger.info("Connected to MongoDB (Aggregation Layer)")

    def _load_clean_dataframe(self) -> pd.DataFrame:
        """Load clean-layer MongoDB data into Pandas"""
        logger.info("Loading clean data into Pandas DataFrame")

        cursor = self.clean_collection.find({}, {"_id": 0})
        df = pd.DataFrame(list(cursor))

        if df.empty:
            raise ValueError("Clean collection is empty")

        df["year"] = df["date_recorded"].str.slice(0, 4).astype(int)
        return df

    # -----------------------------
    # GOLD LAYER 1: YEARLY SUMMARY
    # -----------------------------
    def aggregate_by_year(self) -> int:
        df = self._load_clean_dataframe()

        yearly = (
            df.groupby("year")
            .agg(
                avg_sale_amount=("sale_amount", "mean"),
                total_sales=("sale_amount", "sum"),
                sale_count=("sale_amount", "count"),
            )
            .reset_index()
        )

        yearly["avg_sale_amount"] = yearly["avg_sale_amount"].round(2)

        records = yearly.to_dict(orient="records")

        self.yearly_collection.drop()
        self.yearly_collection.insert_many(records)

        logger.info(f"Yearly gold records created: {len(records)}")
        return len(records)

    # -----------------------------
    # GOLD LAYER 2: TOWN SUMMARY
    # -----------------------------
    def aggregate_by_town(self) -> int:
        df = self._load_clean_dataframe()

        town = (
            df.groupby("town")
            .agg(
                avg_sale_amount=("sale_amount", "mean"),
                total_sales=("sale_amount", "sum"),
                sale_count=("sale_amount", "count"),
            )
            .reset_index()
            .sort_values("total_sales", ascending=False)
        )

        town["avg_sale_amount"] = town["avg_sale_amount"].round(2)

        records = town.to_dict(orient="records")

        self.town_collection.drop()
        self.town_collection.insert_many(records)

        logger.info(f"Town gold records created: {len(records)}")
        return len(records)

    # -----------------------------
    # GOLD LAYER 3: PROPERTY TYPE
    # -----------------------------
    def aggregate_by_property_type(self) -> int:
        df = self._load_clean_dataframe()

        prop = (
            df.groupby("property_type")
            .agg(
                avg_sale_amount=("sale_amount", "mean"),
                sale_count=("sale_amount", "count"),
            )
            .reset_index()
            .sort_values("sale_count", ascending=False)
        )

        prop["avg_sale_amount"] = prop["avg_sale_amount"].round(2)

        records = prop.to_dict(orient="records")

        self.property_collection.drop()
        self.property_collection.insert_many(records)

        logger.info(f"Property gold records created: {len(records)}")
        return len(records)

    def close(self) -> None:
        self.client.close()
