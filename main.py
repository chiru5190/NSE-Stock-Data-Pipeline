import os
import logging
from src.extract import fetch_stock_data
from src.transform import clean_stock_data
from src.load import setup_database, load_data_to_db
from src.analyze import run_analytics

# Configure logging for the main pipeline execution
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_pipeline(symbol: str):
    """
    Executes the end-to-end data engineering pipeline.
    """
    # Define paths
    db_path = "stock_data.db"
    table_name = "daily_prices"
    output_dir = "data/outputs"

    logger.info(f"--- Starting pipeline for symbol: {symbol} ---")

    try:
        # Phase 1: Extract
        logger.info("Phase 1: Extraction")
        raw_data = fetch_stock_data(symbol)
        
        # Phase 2: Transform
        logger.info("Phase 2: Transformation")
        df_clean = clean_stock_data(raw_data)
        
        # Phase 3: Load
        logger.info("Phase 3: Database Storage")
        setup_database(db_path)
        load_data_to_db(df_clean, db_path, table_name, symbol)
        
        # Phase 4: Analytics
        logger.info("Phase 4: Analytics and Export")
        run_analytics(db_path, output_dir, table_name)
        
        logger.info(f"--- Pipeline completed successfully for {symbol} ---")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)

if __name__ == "__main__":
    # Ensure current working directory is correct relative to the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    # Run pipeline for a valid Indian stock
    run_pipeline("TCS")
