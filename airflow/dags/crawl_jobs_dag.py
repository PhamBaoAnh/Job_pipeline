from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import từ src/ nhờ PYTHONPATH đã cấu hình trong docker-compose.yml
from spark.src.elt.etract.jobs_topcv import extract_topcv_jobs
from spark.src.elt.etract.jobs_vietnamworkcv import extract_vienamwork_jobs
from spark.src.elt.load.upload_jobs_to_minio import  upload_jobs_to_minio

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="jobs_crawler_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 7, 24),
    catchup=False,
    tags=["crawler"],
) as dag:

    extract_topcv = PythonOperator(
        task_id="extract_topcv_jobs", 
        python_callable=extract_topcv_jobs,
    )

    extract_vietnamworkcv = PythonOperator(
        task_id="extract_vietnamworks_jobs",
        python_callable=extract_vienamwork_jobs,
    )

    upload_jobs = PythonOperator(
        task_id="upload_jobs_to_gcs",
        python_callable= upload_jobs_to_minio,
    )

    
    [extract_topcv,extract_vietnamworkcv]  >> upload_jobs
