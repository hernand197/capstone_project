import logging
from src.raw_data import RawDataLoader
from src.clean import DataCleaner

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main() -> None:
    """Run the data pipeline"""
    mongo_uri = "mongodb://localhost:27017"
    db_name = "real_estate_db"
    csv_path = "data/Real_Estate_Sales_2001-2023_GL.csv"
    
    logger.info("=" * 50)
    logger.info("Starting data pipeline")
    logger.info("=" * 50)
    
    # RAW LAYER
    logger.info("\n### Raw Layer ###")
    loader = RawDataLoader(mongo_uri, db_name)
    raw_stats = loader.load_csv(csv_path)
    print(f"\nRaw Layer Stats:")
    print(f"  Row Count: {raw_stats['row_count']}")
    print(f"  Schema: {raw_stats['schema']}")
    loader.close()
    
    # CLEAN LAYER
    logger.info("\n### Clean Layer ###")
    cleaner = DataCleaner(mongo_uri, db_name)
    clean_count = cleaner.clean()
    clean_stats = cleaner.get_stats()
    print(f"\nClean Layer Stats:")
    print(f"  Row Count: {clean_stats['row_count']}")
    print(f"  Sample Record: {clean_stats['sample'][0] if clean_stats['sample'] else 'None'}")
    cleaner.close()
    
    logger.info("=" * 50)
    logger.info("Pipeline completed")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()