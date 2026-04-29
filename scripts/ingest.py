import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import shutil
from datetime import date
from utils.logger import get_logger
from scripts.incremental import is_already_processed, mark_as_processed

logger = get_logger("ingest")

RAW_SOURCE = "input_data/sales.csv"
DATA_LAKE = "data/raw/"

def ingest_data(run_date: str = None):
    run_date = run_date or str(date.today())

    if is_already_processed(run_date):
        logger.info(f"Skipping ingest — {run_date} already processed (incremental)")
        return

    try:
        os.makedirs(DATA_LAKE, exist_ok=True)
        destination_file = os.path.join(DATA_LAKE, "sales.csv")
        shutil.copy(RAW_SOURCE, destination_file)
        logger.info(f"Data ingested to {destination_file} for date {run_date}")
    except FileNotFoundError as e:
        logger.error(f"Source file not found: {e}")
        raise
    except Exception as e:
        logger.error(f"Ingest failed: {e}")
        raise

if __name__ == "__main__":
    ingest_data()
