import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from dotenv import load_dotenv
from datetime import datetime

# Env Setup
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

load_dotenv()

# Spark 3.3.4 Session With Windows bypass
spark = SparkSession.builder \
    .appName("LuminaCare_Cleaning") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("fs.s3a.fast.upload", "true") \
    .config("fs.s3a.fast.upload.buffer", "bytebuffer") \
    .config("spark.hadoop.fs.s3a.committer.name", "magic") \
    .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true") \
    .getOrCreate()

# AWS Setup
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", os.getenv("ACCESS_KEY"))
hadoop_conf.set("fs.s3a.secret.key", os.getenv("SECRET_KEY"))
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

def clean_data():

    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day

    day_str = str(day).zfill(2)
   
    path_raw = f"s3a://luminacare-datalake-luis/raw/year={year}/month={month}/day={day}/*.json"    
    df = spark.read.option("multiline", "true").json(path_raw)
    
    print("📊 Raw schema: ")
    df.printSchema()

    # 1. INITIAL FILTER
    print("🧹 Filtering out records with missing IDs or corrupt dates...")
    df_filtered = df.filter(
        (col("ticket_id").isNotNull()) & 
        (~col("created_at_format").contains("error"))
    )

    print("✨ Engineering new features and cleaning error strings...")
    # 2. TRANSFORM & RESCUE
    df_final = df_filtered.withColumn(
        "priority", 
        when(col("priority").contains("error"), "Unknown").otherwise(col("priority"))
    ).withColumn(
        "status", 
        when(col("status").contains("error"), "Unknown").otherwise(col("status"))
    ).withColumn(
        "issue_category", 
        when(col("issue_category").contains("error"), "Others").otherwise(col("issue_category"))
    ).withColumn(
        "sentiment_label",
        when(col("customer.sentiment") >= 7, "Positive")
        .when(col("customer.sentiment") >= 4, "Neutral")
        .otherwise("Negative")
    ).select(
        "ticket_id",
        "agent_id",
        "priority",
        "status",
        "response_time_hrs",
        "issue_category",
        "sentiment_label",
        col("created_at_format").alias("event_timestamp")
    )

    # 3. SAVE TO PARQUET
    path_staging = f"s3a://luminacare-datalake-luis/staging/tickets_cleaned/day={day_str}/"
    final_count = df_final.count()
    print(f"📤 Saving {final_count} clean records to Parquet...")

    if final_count > 0:
        df_final.write.mode("overwrite").parquet(path_staging)
        print(f"✅ Success! Transformed data saved in: {path_staging}")
    else:
        print("⚠️ No records to save. Check your source data and filters.")

if __name__ == "__main__":
    clean_data()