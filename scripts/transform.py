import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date
from pyspark.sql.functions import col, sum
from utils.spark_session import create_spark
from utils.logger import get_logger
from scripts.incremental import mark_as_processed

logger = get_logger("transform")

S3_ENABLED = os.getenv("S3_ENABLED", "false").lower() == "true"
S3_BUCKET = os.getenv("S3_BUCKET", "")

def transform_data(run_date: str = None):
    run_date = run_date or str(date.today())
    spark = create_spark()

    try:
        logger.info(f"Starting transformation for date {run_date}")

        df = spark.read.csv("data/raw/sales.csv", header=True, inferSchema=True)
        logger.info(f"Loaded {df.count()} rows from raw CSV")

        df.show()

        df = df.dropna()
        df = df.filter(col("amount") >= 0)
        df = df.dropDuplicates()
        logger.info(f"After cleaning: {df.count()} rows remaining")

        df_agg = df.groupBy("date").agg(sum("amount").alias("total_sales"))

        # Partition by date (real-world optimization)
        output_path = "data/processed/sales"
        os.makedirs(output_path, exist_ok=True)
        df_agg.write.mode("overwrite").partitionBy("date").parquet(output_path)
        logger.info(f"Transformed data written to {output_path} partitioned by date")

        df_agg.show()

        if S3_ENABLED and S3_BUCKET:
            from utils.s3_utils import upload_to_s3
            upload_to_s3(output_path, S3_BUCKET, f"processed/sales/run_date={run_date}")
            logger.info(f"Uploaded processed data to S3 bucket: {S3_BUCKET}")

        mark_as_processed(run_date)
        logger.info(f"Marked {run_date} as processed in incremental tracker")

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    transform_data()
