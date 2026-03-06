import os
import glob
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clear_outputs():
    """
    Deletes the SQLite database and all generated CSV reports to allow a fresh run.
    """
    # 1. Clear the SQLite database
    db_path = "stock_data.db"
    if os.path.exists(db_path):
        os.remove(db_path)
        logging.info(f"Deleted database at '{db_path}'")
    else:
        logging.info("No database found to delete.")

    # 2. Clear the CSV reports in the data/outputs folder
    output_dir = "data/outputs"
    csv_files = glob.glob(f"{output_dir}/*.csv")
    for file in csv_files:
        try:
            os.remove(file)
            logging.info(f"Deleted '{file}'")
        except Exception as e:
            logging.error(f"Failed to delete '{file}': {e}")
            
    if not csv_files:
        logging.info("No CSV reports found to delete.")

if __name__ == "__main__":
    # Ensure script runs from the correct directory relative to its location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    logging.info("--- Starting project cleanup ---")
    clear_outputs()
    logging.info("--- Cleanup completed ---")
