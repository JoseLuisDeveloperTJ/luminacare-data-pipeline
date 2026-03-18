import os
import sys
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

# Windows setup
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# SPARK SESSION 3.3.4
spark = SparkSession.builder \
    .appName("LuminaCare_Inspection") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# AWS setup
ctx = spark._jsc.hadoopConfiguration()
ctx.set("fs.s3a.access.key", os.getenv("ACCESS_KEY"))
ctx.set("fs.s3a.secret.key", os.getenv("SECRET_KEY"))
ctx.set("fs.s3a.endpoint", "s3.amazonaws.com")


path_inspect = "s3a://luminacare-datalake-luis/staging/tickets_cleaned/day=18/*.parquet"

print(" Reading parquet files from S3...")

try:
    df_check = spark.read.parquet(path_inspect)
    
    print("\n--- First 5 Rows ---")
    df_check.show(5, truncate=False)
    
    print("--- Final Schema ---")
    df_check.printSchema()
    
    print(f"✅ Total: {df_check.count()} Records ready for snowflake.")

except Exception as e:
    print(f"❌ We couldnt read the file: {e}")