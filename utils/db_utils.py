import os
import psycopg2
from psycopg2.extras import execute_values
from utils.logger import get_logger

logger = get_logger("db_utils")

def get_connection():
    return psycopg2.connect(
        host=os.getenv("PIPELINE_PG_HOST", "pipeline-postgres"),
        port=os.getenv("PIPELINE_PG_PORT", "5432"),
        dbname=os.getenv("PIPELINE_PG_DB", "sales_db"),
        user=os.getenv("PIPELINE_PG_USER", "postgres"),
        password=os.getenv("PIPELINE_PG_PASSWORD", "postgres")
    )

def ensure_table_exists():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sales_aggregated (
                    id          SERIAL PRIMARY KEY,
                    date        DATE NOT NULL,
                    total_sales NUMERIC(12, 2) NOT NULL,
                    run_date    DATE NOT NULL,
                    loaded_at   TIMESTAMP DEFAULT NOW(),
                    UNIQUE (date, run_date)
                )
            """)
            conn.commit()
            logger.info("Table sales_aggregated is ready")
    finally:
        conn.close()

def upsert_records(records: list[tuple]):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO sales_aggregated (date, total_sales, run_date)
                VALUES %s
                ON CONFLICT (date, run_date) DO UPDATE
                    SET total_sales = EXCLUDED.total_sales,
                        loaded_at   = NOW()
            """, records)
            conn.commit()
            logger.info(f"Upserted {len(records)} records into sales_aggregated")
    finally:
        conn.close()
