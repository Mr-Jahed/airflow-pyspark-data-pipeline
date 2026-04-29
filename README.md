# Sales Batch Data Pipeline

A production-style batch data pipeline built with Apache Airflow, PySpark, and MinIO. Ingests raw sales data daily, cleans and transforms it using Spark, validates the output, and stores processed data as partitioned Parquet files in object storage.

---

## Tech Stack

- Apache Airflow 3.x via Astronomer for orchestration
- PySpark 4.0 for distributed data processing
- MinIO as S3-compatible local object storage
- Docker via Astro CLI for containerized local development
- Python 3.13
- Java 21 (inside container), Java 17 (host machine)

---

## Project Structure

```
data_pipeline_project/
├── dags/
│   └── pipeline_dag.py          # Airflow DAG - defines task flow and schedule
├── scripts/
│   ├── ingest.py                # Copies source data to raw layer
│   ├── transform.py             # PySpark cleaning, aggregation, partitioning
│   ├── validate.py              # Data quality checks on processed output
│   └── incremental.py           # Tracks processed dates to avoid reprocessing
├── utils/
│   ├── spark_session.py         # Spark session factory
│   ├── s3_utils.py              # S3/MinIO upload and download helpers
│   └── logger.py                # Shared logger (file + console)
├── data/
│   ├── raw/                     # Landing zone for ingested CSV
│   └── processed/               # Cleaned parquet output partitioned by date
├── input_data/
│   └── sales.csv                # Source data file
├── logs/
│   └── pipeline.log             # Aggregated pipeline logs
├── Dockerfile                   # Extends Astro runtime with Java
├── docker-compose.override.yml  # Adds MinIO to the Astro environment
├── requirements.txt
└── .env                         # Credentials and config (not committed)
```

---

## Pipeline Flow

```
ingest -> transform -> validate
```

ingest checks the incremental tracker and copies the source CSV to the raw layer if the date has not been processed yet.

transform reads the raw CSV via Spark, drops nulls, filters negative amounts, deduplicates rows, aggregates total sales by date, writes output as Parquet partitioned by date, and uploads to MinIO if S3 is enabled.

validate reads the processed Parquet back via Spark and asserts no nulls, no negatives, and non-empty output.

---

## Local Setup

### Prerequisites

- Docker Desktop running
- Astronomer CLI installed
- Java 17 on host machine with JAVA_HOME set correctly
- winutils.exe in C:\hadoop\bin and HADOOP_HOME=C:\hadoop (Windows only)

### Environment Variables

Copy the example below into a .env file in the project root and fill in your values.

```
S3_ENABLED=true
S3_BUCKET=data-pipeline
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
MLFLOW_S3_ENDPOINT_URL=http://minio:9000
S3_ENDPOINT_URL=http://minio:9000
```

For real AWS S3, replace the credentials with IAM access keys and remove the endpoint URL variables.

### Start the Project

```bash
docker rm -f minio
astro dev start --wait 5m
```

This starts Airflow and MinIO together. Airflow UI is at http://localhost:8080 (admin/admin). MinIO UI is at http://localhost:9001 (minioadmin/minioadmin).

### Run Scripts Locally Without Airflow

```bash
python scripts/ingest.py
python scripts/transform.py
python scripts/validate.py
```

---

## Incremental Processing

The pipeline tracks processed dates in data/incremental_tracker.json. If a date is already recorded, the ingest step skips that run. To reprocess a specific date, remove it from the tracker file or delete the file entirely to reset.

---

## Storage Layout in MinIO / S3

Processed files are uploaded using Hive-style partitioning:

```
data-pipeline/
└── processed/
    └── sales/
        └── run_date=2026-04-30/
            └── part-00000-xxxx.snappy.parquet
```

This layout is compatible with AWS Athena, Spark, Presto, and most modern query engines out of the box.

---

## DAG Configuration

The DAG runs daily on a schedule. It has 2 retries with a 5 minute delay between attempts. Catchup is disabled so it only runs for the current date when triggered manually.

---

## Logs

All pipeline activity is written to logs/pipeline.log with timestamps, log levels, and the name of the script that generated the log. Task logs are also visible in the Airflow UI under each task run.

---

## Switching to Real AWS S3

When you have AWS credentials, update .env as follows:

```
S3_ENABLED=true
S3_BUCKET=your-bucket-name
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
```

Remove the MLFLOW_S3_ENDPOINT_URL and S3_ENDPOINT_URL lines. No code changes needed.

---

## Known Limitations

- Pipeline currently processes a single static CSV file. In a real setup this would be replaced with a source database connection or an API pull.
- Spark runs in local mode. For large data volumes this should be pointed at a proper Spark cluster or replaced with EMR/Databricks.
- The incremental tracker uses a local JSON file. In a multi-worker Airflow setup this should be moved to a database table or a shared storage location.
