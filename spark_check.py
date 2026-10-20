
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RidershipEnvironmentCheck")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

try:
    df = spark.createDataFrame(
        [("Route 1", 1200), ("Route 2", 850)],
        ["route", "rides"]
    )
    df.show()
    print("Spark version:", spark.version)
    print("Spark is working!")
finally:
    spark.stop()
