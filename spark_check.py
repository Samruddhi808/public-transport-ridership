import os
import sys
import time

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

print(f"Starting SparkSession with Python: {sys.executable}")
start_time = time.time()
spark = (
    SparkSession.builder
    .appName("RidershipEnvironmentCheck")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
print(f"SparkSession created in {time.time() - start_time:.2f}s, Spark version: {spark.version}")

try:
    print("Testing SQL query...")
    df_sql = spark.sql("SELECT 1 AS id, 'Test' AS name")
    rows = df_sql.collect()
    print("SQL collect success:", rows)
    
    print("Testing createDataFrame...")
    df = spark.createDataFrame(
        [("Route 1", 1200), ("Route 2", 850)],
        ["route", "rides"]
    )
    df.show()
    print("Spark is working!")
finally:
    spark.stop()
    print("Spark stopped cleanly.")
