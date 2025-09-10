from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import col, when, size, lit, element_at
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
import os
import sys
sys.path.append("/opt/airflow/spark/src")  
from spark.src.elt.load.upload_jobs_to_minio import upload_jobs_cleaned_to_minio


MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minio123')
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'job')


spark = SparkSession.builder \
    .appName("SilverTopCVTransformTest") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet(f"s3a://{MINIO_BUCKET}/bronze/topcv/topcv_jobs.parquet")

df = (
    df.withColumnRenamed("title", "job_position")
      .withColumnRenamed("work_address", "job_location")
      .withColumn("job_position", F.trim(F.lower("job_position")))
      .withColumn("company_name", F.trim("company_name"))
      .withColumn("salary_clean", F.lower(F.col("salary")))
      .withColumn("job_location", F.trim(F.lower("job_location")))
      .withColumn("clean_deadline", F.regexp_replace("deadline_submit", "Hạn nộp hồ sơ: ", ""))
      .withColumn("job_posting_date", F.to_date("clean_deadline", "dd/MM/yyyy"))
      .withColumn("yoe_clean", F.trim(F.lower("yoe")))
      .withColumn("company_size", F.trim(F.lower("company_size")))
      .withColumn("num_of_recruit", F.regexp_replace("num_of_recruit", " người", "").cast("int"))
      .withColumn("relation_fields", F.array_remove("relation_fields", "Việc làm IT"))
      # Lấy city & district an toàn
      .withColumn("city", when(size(col("area")) >= 1, element_at(col("area"), 1)).otherwise(lit(None)))
      .withColumn("district", when(size(col("area")) >= 2, element_at(col("area"), 2)).otherwise(lit(None)))
      .withColumn("source", F.lit("topcv"))
      .withColumn("ingest_time", F.current_timestamp())
)

df = df.withColumn(
    "yoe_standard",
    F.when(F.col("yoe_clean").contains("không yêu cầu"), F.lit(0))
     .when(F.col("yoe_clean").contains("trên"), F.lit(5.5))
     .when(F.col("yoe_clean").rlike(r"\d+"), F.regexp_extract("yoe_clean", r"(\d+)", 1).cast("int"))
)

df = (
    df.withColumn(
        "min_size",
        F.regexp_extract("company_size", r"(\d+)", 1).cast("int")
    )
    .withColumn(
        "max_size",
        F.when(F.col("company_size").contains("+"), F.col("min_size"))  
         .when(~F.col("company_size").contains("-"), F.col("min_size")) 
         .otherwise(F.regexp_extract("company_size", r"-(\d+)", 1).cast("int"))
    )
)

skill_keywords = [
    "python", "java", "scala", "sql", "javascript", "html", "css",
    "react", "node", "angular", "spring", "docker", "kubernetes",
    "aws", "azure", "gcp", "git", "linux", "rest", "graphql",
    "c#", "c++", "flutter", "android", "ios",
    "spark", "hadoop", "kafka", "hive", "hbase", "airflow", "odoo", "nosql",
    "elasticsearch", "zookeeper", "cassandra", "mongodb", "redis",
    "flink", "beam", "storm",
    "tensorflow", "pytorch", "mlflow",
    "jupyter", "zeppelin",
    "etl", "data pipeline", "data warehouse", "data lake",
    "bigquery", "redshift", "snowflake",
    "bash", "shell", "powershell",
    "ruby", "perl", "matlab",
    "jenkins", "circleci", "travisci",
    "prometheus", "grafana",
    "api", "microservices", "serverless"
]

skills_broadcast = spark.sparkContext.broadcast([s.lower() for s in skill_keywords])

def extract_skills(req):
    if not req:
        return []
    req_lower = req.lower()
    return [skill for skill in skills_broadcast.value if skill in req_lower]

extract_skills_udf = F.udf(extract_skills, ArrayType(StringType()))
df = df.withColumn("skills_extracted", extract_skills_udf("requirements"))

df = df.withColumn(
    "min_salary",
    F.when(F.col("salary_clean").contains("thỏa thuận"), None)
     .when(F.col("salary_clean").rlike(r"\d+"), F.regexp_extract("salary_clean", r"(\d+)", 1).cast("int") * 1_000_000)
)

df = df.withColumn(
    "max_salary",
    F.when(F.col("salary_clean").contains("thỏa thuận"), None)
     .when(F.col("salary_clean").rlike(r"\d+\s*-\s*(\d+)"), F.regexp_extract("salary_clean", r"\d+\s*-\s*(\d+)", 1).cast("int") * 1_000_000)
)

df = df.filter(
    (col("job_position").isNotNull()) & (col("job_position") != "") &
    (col("company_name").isNotNull()) & (col("company_name") != "") &
    (col("requirements").isNotNull()) & (col("requirements") != "") &
    (col("description").isNotNull()) & (col("description") != "")
)

df_silver = df.select(
    "job_position", 
    "company_name", 
    F.col("yoe_standard").alias("yoe"), 
    "job_posting_date",
    "min_salary",
    "max_salary",
    F.col("min_size").alias("company_size_min"),
    F.col("max_size").alias("company_size_max"),
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

window_spec = Window.partitionBy("job_position", "company_name").orderBy(desc("job_posting_date"))

df_silver_dedup = (
    df_silver
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

df_silver_dedup.show()
upload_jobs_cleaned_to_minio(df_silver_dedup, source_name="topcv", layer="silver")
spark.stop()
