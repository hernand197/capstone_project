from pymongo import MongoClient
import logging

logger = logging.getLogger(__name__)

def create_indexes(mongo_uri: str, db_name: str) -> None:
    client = MongoClient(mongo_uri)
    db = client[db_name]

    logger.info("Creating MongoDB indexes")

    db.real_estate_clean.create_index("date_recorded")
    db.real_estate_clean.create_index("town")
    db.real_estate_clean.create_index("property_type")

    db.real_estate_clean.create_index(
        [("address", 1), ("date_recorded", 1)],
        unique=True
    )

    db.real_estate_gold_yearly.create_index("year")
    db.real_estate_gold_town.create_index("town")
    db.real_estate_gold_property.create_index("property_type")

    client.close()
