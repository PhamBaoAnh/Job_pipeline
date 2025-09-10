from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    monotonically_increasing_id, col, explode, trim, hash,
    dayofmonth, month, year, quarter, to_date, coalesce, lit
)
import sys
import os

sys.path.append("/opt/airflow/spark/src")  # mount trong docker-compose

from elt.load.upload_jobs_to_minio import upload_jobs_cleaned_to_minio

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minio123')
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'job')


spark = SparkSession.builder \
    .appName("SilverTopCVTransformTest") \
    .master("local[*]") \
    .getOrCreate()


df1 = spark.read.parquet(f"s3a://{MINIO_BUCKET}/silver/topcv/topcv_jobs.parquet")
df2 = spark.read.parquet(f"s3a://{MINIO_BUCKET}/silver/vietnamworkcv/vietnamworkcv_jobs.parquet")


df = df1.unionByName(df2)


df = df.withColumn("JobPostingID", hash(col("company_name"), col("job_position"), col("job_posting_date")))

df = df.withColumn("DateID", to_date(col("job_posting_date")))

df = df.withColumn("work_form", coalesce(col("work_form"), lit(""))).withColumn("work_form", col("work_form").cast("string"))


dim_company = (
    df.select(col("company_name").alias("CompanyName"), col("major_field").alias("MajorField"))
      .dropDuplicates()
      .withColumn("CompanyID", monotonically_increasing_id())
)


dim_jobposition = (
    df.select(
        col("job_position").alias("JobPosition"),
        col("level").alias("Level"),
        col("relation_fields").alias("RelationFields")
    )
    .dropDuplicates()
    .withColumn("JobPositionID", monotonically_increasing_id())
)


dim_date = (
    df.select(col("DateID"))
      .dropDuplicates()
      .withColumn("Day", dayofmonth(col("DateID")))
      .withColumn("Month", month(col("DateID")))
      .withColumn("Quarter", quarter(col("DateID")))
      .withColumn("Year", year(col("DateID")))
)


dim_workform = (
    df.select(col("work_form").alias("WorkForm"))
      .dropDuplicates()
      .withColumn("WorkFormID", monotonically_increasing_id())
)


fact_job = (
    df.alias("f")
      .join(dim_company.alias("dc"), col("f.company_name") == col("dc.CompanyName"), "left")
      .join(dim_jobposition.alias("dj"), col("f.job_position") == col("dj.JobPosition"), "left")
      .join(dim_date.alias("dd"), col("f.DateID") == col("dd.DateID"), "left")
      .join(dim_workform.alias("dw"), col("f.work_form") == col("dw.WorkForm"), "left")
      .select(
          col("f.JobPostingID"),
          col("dc.CompanyID"),
          col("dj.JobPositionID"),
          col("dd.DateID"),
          col("dw.WorkFormID"),
          col("f.min_salary").alias("MinSalary"),
          col("f.max_salary").alias("MaxSalary"),
          col("f.yoe").alias("YOE"),
          col("f.num_of_recruit").alias("NumOfRecruit")
      )
)

dim_skills = (
    df.select(explode(col("skills_extracted")).alias("SkillName"))
      .withColumn("SkillName", trim(col("SkillName")))
      .dropDuplicates()
      .withColumn("SkillID", monotonically_increasing_id())
)


job_skills = (
    df.select(col("JobPostingID"), explode(col("skills_extracted")).alias("SkillName"))
      .withColumn("SkillName", trim(col("SkillName")))
)

fact_jobskill = (
    job_skills.join(dim_skills, "SkillName", "left")
              .select("JobPostingID", "SkillID")
              .dropDuplicates()
)

dfs_to_upload = [
    ("dim_company", dim_company),
    ("dim_jobposition", dim_jobposition),
    ("dim_date", dim_date),
    ("dim_workform", dim_workform),
    ("dim_skills", dim_skills),
    ("fact_job", fact_job),
    ("fact_jobskill", fact_jobskill),
    ("view",df)

]

for name, df in dfs_to_upload:
    upload_jobs_cleaned_to_minio(df, source_name=name, layer="gold")

spark.stop()
