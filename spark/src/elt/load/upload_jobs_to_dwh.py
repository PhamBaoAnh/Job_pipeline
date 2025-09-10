from pyspark.sql import SparkSession
import duckdb
import os

api_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImJhb2FuaDdhQGdtYWlsLmNvbSIsInNlc3Npb24iOiJiYW9hbmg3YS5nbWFpbC5jb20iLCJwYXQiOiJBYUMzdUVZdXdEc3E3b0RuQVpqRmpsSlVOTERiOWxPZnpDcDRQUXJ4RDdNIiwidXNlcklkIjoiZmRiMmJkZDAtMTdjYS00ZGIzLTg5MzItMTY2ZWEwZGJkYzcwIiwiaXNzIjoibWRfcGF0IiwicmVhZE9ubHkiOmZhbHNlLCJ0b2tlblR5cGUiOiJyZWFkX3dyaXRlIiwiaWF0IjoxNzU3NDM3ODUzfQ.MyN0hvVw5azfQng-2pnmP4qsjmghq1E-FKERky8lPRM'

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minio123')
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'job')

spark = SparkSession.builder \
    .appName("Load_data_to_dwh") \
    .master("local[*]") \
    .getOrCreate()

tables = [
    "dim_company",
    "dim_jobposition",
    "dim_date",
    "dim_workform",
    "dim_skills",
    "fact_job",
    "fact_jobskill",
    "view"
]

con = duckdb.connect(
    database='md:jobs',
    config={'motherduck_token': api_key}
)

for table in tables:
    gcs_path = f"s3a://{MINIO_BUCKET}/gold/{table}/{table}_jobs.parquet"
    
    df_spark = spark.read.parquet(gcs_path)
    df_pd = df_spark.toPandas()
    
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM df_pd")

    print(f"{table} loaded successfully!")

spark.stop()
con.close()

