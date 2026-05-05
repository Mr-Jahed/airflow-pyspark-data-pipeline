import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date
from utils.logger import get_logger
from utils.db_utils import ensure_table_exists, upsert_records
from utils.spark_session import create_spark

logger = get_logger("load")

def load_data(run_date: str = None):
    run_date = run_date or str(date.today())
    spark = create_spark()

    try:
        logger.info(f"Starting load for run_date {run_date}")

        ensure_table_exists()

        df = spark.read.parquet("data/processed/sales")
        rows = df.collect()

        if not rows:
            raise ValueError("No data found in processed parquet to load")

        records = [(row["date"], float(row["total_sales"]), run_date) for row in rows]

        upsert_records(records)

        logger.info(f"Load complete — {len(records)} rows loaded into PostgreSQL for run_date {run_date}")

    except Exception as e:
        logger.error(f"Load failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    load_data()
