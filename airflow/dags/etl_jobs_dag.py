from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import os

# Biến môi trường MinIO
MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minio123')
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'job')

# Cấu hình Spark chung cho MinIO (S3A)
s3a_conf = {
    "spark.master": "spark://spark-master:7077",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}


with DAG(
    dag_id='spark_transform_pipeline',
    start_date=datetime(2025, 8, 4),
    schedule_interval=None,
    catchup=False,
) as dag:
    # TaskGroup cho tất cả job Silver
    with TaskGroup("silver_group", tooltip="All silver transformations") as silver_group:

        silver_topcv_transform = SparkSubmitOperator(
            task_id='silver_topcv_transform',
            conn_id='spark_default',
            application='/opt/spark-jobs/elt/transform/silver_topcv_transform.py',
            packages="org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.20.15",
            conf=s3a_conf,
            verbose=True,
        )

        silver_vietnamworkcv_transform = SparkSubmitOperator(
            task_id='silver_vietnamworkcv_transform',
            conn_id='spark_default',
            application='/opt/spark-jobs/elt/transform/silver_vietnamworkcv_transform.py',
            packages="org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.20.15",
            conf=s3a_conf,
            verbose=True,
        )

 
    gold_transform = SparkSubmitOperator(
        task_id='gold_transform',
        conn_id='spark_default',
        application='/opt/spark-jobs/elt/transform/gold_transform.py',
        packages="org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.20.15",
        conf=s3a_conf,
        verbose=True,
    )
    
    load_data_to_dwh = SparkSubmitOperator(
        task_id='load_data_to_dwh',
        conn_id='spark_default',
        application='/opt/spark-jobs/elt/load/upload_jobs_to_dwh.py',
        packages="org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.20.15",
        conf=s3a_conf,
        verbose=True,
    )

    # Thiết lập thứ tự thực thi
    silver_group >> gold_transform >> load_data_to_dwh
