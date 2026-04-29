import boto3
import os
from utils.logger import get_logger

logger = get_logger("s3_utils")

def get_s3_client():
    endpoint_url = os.getenv("MLFLOW_S3_ENDPOINT_URL")
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        endpoint_url=endpoint_url
    )

def ensure_bucket_exists(bucket: str):
    s3 = get_s3_client()
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Created bucket: {bucket}")

def upload_to_s3(local_path: str, bucket: str, s3_prefix: str):
    s3 = get_s3_client()
    ensure_bucket_exists(bucket)
    for root, _, files in os.walk(local_path):
        for file in files:
            if file.startswith(".") or file.startswith("_"):
                continue
            full_path = os.path.join(root, file)
            relative = os.path.relpath(full_path, local_path)
            s3_key = f"{s3_prefix}/{relative}".replace("\\", "/")
            s3.upload_file(full_path, bucket, s3_key)
            logger.info(f"Uploaded {full_path} to s3://{bucket}/{s3_key}")

def download_from_s3(bucket: str, s3_key: str, local_path: str):
    s3 = get_s3_client()
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(bucket, s3_key, local_path)
    logger.info(f"Downloaded s3://{bucket}/{s3_key} to {local_path}")
