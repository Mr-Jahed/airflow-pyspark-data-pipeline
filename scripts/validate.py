import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.functions import col
from utils.spark_session import create_spark
from utils.logger import get_logger

logger = get_logger("validate")

def validate_data():
    spark = create_spark()

    try:
        logger.info("Starting data validation")

        df = spark.read.parquet("data/processed/sales")
        df.show()

        null_count = df.filter(col("total_sales").isNull()).count()
        assert null_count == 0, f"Null values found in total_sales: {null_count} rows"

        neg_count = df.filter(col("total_sales") < 0).count()
        assert neg_count == 0, f"Negative values found in total_sales: {neg_count} rows"

        row_count = df.count()
        assert row_count > 0, "Validation failed: processed data is empty"

        logger.info(f"Validation passed — {row_count} rows, no nulls, no negatives")

    except AssertionError as e:
        logger.error(f"Validation check failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Validation error: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    validate_data()
