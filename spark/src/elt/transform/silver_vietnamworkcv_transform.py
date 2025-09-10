from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, when, lit, regexp_extract, transform, row_number, desc
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType
)
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
import os
import sys
sys.path.append("/opt/airflow/spark/src")  # mount trong docker-compose
from spark.src.elt.load.upload_jobs_to_minio import upload_jobs_cleaned_to_minio

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minio123')
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'job')


spark = SparkSession.builder \
    .appName("TransformTest") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet(f"s3a://{MINIO_BUCKET}/bronze/vietnamworkcv/vietnamworkcv_jobs.parquet")


# UDF parse company size
@udf(returnType=StructType([
    StructField("min", IntegerType(), True),
    StructField("max", IntegerType(), True)
]))
def parse_company_size(size_str):
    if not size_str:
        return None
    try:
        size_str = size_str.replace(",", "").replace("+", "")
        parts = size_str.strip().split("-")
        min_size = int(parts[0])
        max_size = int(parts[1]) if len(parts) > 1 else None
        return {"min": min_size, "max": max_size}
    except:
        return None

# Transform
df_transformed = df.select(
    col("jobTitle").alias("job_position"),
    col("companyName").alias("company_name"),
    col("yearsOfExperience").alias("yoe"),
    to_date("approvedOn").alias("job_posting_date"),
    col("salaryMin").alias("min_salary"),
    col("salaryMax").alias("max_salary"),
    parse_company_size(col("companySize")).alias("company_size_parsed"),
    col("salaryCurrency").alias("salary_currency"),
    col("jobFunction.parentName").alias("major_field"),
    col("jobLevelVI").alias("level"),
    col("numberOfRecruits").alias("num_of_recruit"),
    when(col("typeWorkingId") == 1, "Toàn thời gian").otherwise("Bán thời gian").alias("work_form"),
    transform(col("jobFunction.children"), lambda x: x["name"]).alias("relation_fields"),
    col("workingLocations")[0]["cityNameVI"].alias("city"),
    regexp_extract(col("workingLocations")[0]["address"], r"(?i)(Quận|Q\.)\s*([^\.,\d]+)", 2).alias("district"),
    lit("vietnamworks").alias("source"),
    transform(col("skills"), lambda x: x["skillName"]).alias("skills_extracted")
)

# Currency conversion
EXCHANGE_RATE = 24000
df_transformed = (
    df_transformed
    .withColumn(
        "min_salary",
        when((col("salary_currency") == "USD") & col("min_salary").isNotNull() & (col("min_salary") != 0),
             col("min_salary") * EXCHANGE_RATE
        ).when((col("salary_currency") == "VND") & col("min_salary").isNotNull() & (col("min_salary") != 0),
               col("min_salary")
        ).otherwise(lit(None))
    )
    .withColumn(
        "max_salary",
        when((col("salary_currency") == "USD") & col("max_salary").isNotNull() & (col("max_salary") != 0),
             col("max_salary") * EXCHANGE_RATE
        ).when((col("salary_currency") == "VND") & col("max_salary").isNotNull() & (col("max_salary") != 0),
               col("max_salary")
        ).otherwise(lit(None))
    )
    .withColumn("company_size_min", col("company_size_parsed")["min"])
    .withColumn("company_size_max", col("company_size_parsed")["max"])
)

# Final select
df_silver = df_transformed.select(
    "job_position",
    "company_name",
    "yoe",
    "job_posting_date",
    "min_salary",
    "max_salary",
    "company_size_min",
    "company_size_max",
    "major_field",
    "level",
    "num_of_recruit",
    "work_form",
    "relation_fields",
    "skills_extracted",
    "city",
    "district",
    "source"
)

# Loại bỏ trùng: giữ bản ghi mới nhất theo job_posting_date cho mỗi job_position + company_name
window_spec = Window.partitionBy("job_position", "company_name").orderBy(desc("job_posting_date"))

df_silver_dedup = (
    df_silver
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)


upload_jobs_cleaned_to_minio(df_silver_dedup, source_name="vietnamworkcv", layer="silver")
df_silver_dedup.show()
spark.stop()
