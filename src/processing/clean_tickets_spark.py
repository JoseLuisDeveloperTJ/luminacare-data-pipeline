import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower
from dotenv import load_dotenv

# Env Setup
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

load_dotenv()

# Spark 3.5.5 Session
spark = SparkSession.builder \
    .appName("LuminaCare_Cleaning") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# AWS Setup
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", os.getenv("ACCESS_KEY"))
hadoop_conf.set("fs.s3a.secret.key", os.getenv("SECRET_KEY"))
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

def clean_data():
    path_raw = "s3a://luminacare-datalake-luis/raw/year=2026/month=3/day=17/*.json"
    
    print("Reading data from S3...")
    df = spark.read.json(path_raw)
    df.printSchema()

    # Esto nos dirá qué columnas existen realmente
    print("Real Columns found:", df.columns)
    df.show(5, truncate=False) # Ver el contenido real

    df_clean = df.dropna(subset=["ticket_id"]) \
                 .withColumn("sentiment", lower(col("customer_sentiment")))

    path_staging = "s3a://luminacare-datalake-luis/staging/tickets_cleaned/"
    df_clean.write.mode("overwrite").parquet(path_staging)
    print(f"Success! Data saved in: {path_staging}")

if __name__ == "__main__":
    clean_data()