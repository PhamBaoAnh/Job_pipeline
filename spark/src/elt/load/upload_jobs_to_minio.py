import os
import pandas as pd
from io import BytesIO
import boto3
from spark.src.elt.etract.jobs_topcv import extract_topcv_jobs
from spark.src.elt.etract.jobs_vietnamworkcv import extract_vienamwork_jobs

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minio123')
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'job')


s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

def ensure_bucket_exists(bucket_name):
    existing_buckets = [b['Name'] for b in s3_client.list_buckets()['Buckets']]
    if bucket_name not in existing_buckets:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' created.")


def upload_jobs_to_minio():
    ensure_bucket_exists(MINIO_BUCKET)
    dataframes = {
        "topcv": extract_topcv_jobs(),
        "vietnamworkcv": extract_vienamwork_jobs()
    }

    for name, df in dataframes.items():
        if df is None or df.empty:
            print(f"⚠️ DataFrame '{name}' rỗng hoặc không tồn tại.")
            continue

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        object_name = f"bronze/{name}/{name}_jobs.parquet"

        s3_client.put_object(
            Bucket=MINIO_BUCKET,
            Key=object_name,
            Body=buffer,
        )
        print(f"✅ Uploaded Pandas DataFrame to MinIO: {MINIO_BUCKET}/{object_name}")


def upload_jobs_cleaned_to_minio(spark_df, source_name, layer="bronze", path_prefix=""):
    """
    Lưu Spark DataFrame trực tiếp vào MinIO qua s3a
    """
    path = f"s3a://{MINIO_BUCKET}/{layer}/{path_prefix}{source_name}/{source_name}_jobs.parquet"
    spark_df.write.mode("overwrite").parquet(path)
    print(f"✅ Uploaded Spark DataFrame to MinIO: {path}")
