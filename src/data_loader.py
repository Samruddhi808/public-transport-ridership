"""
Data ingestion and validation module using Apache PySpark.

Handles CSV loading with explicit schema, strict data quality checks,
duplicate detection, and quarantining of anomalous or corrupted records.
"""

from pathlib import Path
from typing import Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

# Explicit raw ingestion schema
RAW_SCHEMA = StructType([
    StructField("route_id", StringType(), False),
    StructField("route_name", StringType(), False),
    StructField("service_date", StringType(), False),
    StructField("day_type", StringType(), False),
    StructField("rides", IntegerType(), False),
])

VALID_DAY_TYPES = ["Weekday", "Saturday", "Sunday/Holiday"]


def load_raw_ridership(spark: SparkSession, csv_path: str | Path) -> DataFrame:
    """
    Read raw ridership CSV file with an explicit PySpark schema.
    Raises FileNotFoundError if the file does not exist.
    """
    path_obj = Path(csv_path)
    if not path_obj.exists():
        raise FileNotFoundError(
            f"Raw ridership dataset not found at '{path_obj.resolve()}'. "
            "Please generate it first by running: python scripts/generate_data.py"
        )

    # Use resolved POSIX path for Spark compatibility
    spark_path = path_obj.resolve().as_posix()
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(RAW_SCHEMA)
        .load(spark_path)
    )
    return df


def validate_and_clean_data(
    df: DataFrame,
    quarantine_output_path: Optional[str | Path] = None
) -> Tuple[DataFrame, DataFrame]:
    """
    Perform comprehensive data quality validation:
    1. Parse service_date to Spark DateType and check for unparseable dates.
    2. Check for null or empty values in mandatory fields.
    3. Check for negative ride counts.
    4. Check for invalid day_type classifications.
    5. Detect duplicate (route_id, service_date) records.
    
    Returns:
        (clean_df, quarantine_df)
    """
    # 1. Standardize and parse service_date with regex guard to prevent parse exceptions
    df_parsed = (
        df
        .withColumn(
            "parsed_date",
            F.when(
                F.col("service_date").rlike(r"^\d{4}-\d{2}-\d{2}$"),
                F.to_date(F.col("service_date"), "yyyy-MM-dd")
            ).otherwise(None)
        )
        .withColumn("route_id", F.trim(F.col("route_id")))
        .withColumn("route_name", F.trim(F.col("route_name")))
        .withColumn("day_type", F.trim(F.col("day_type")))
    )

    # 2. Check for duplicate keys: (route_id, parsed_date)
    dup_window = Window.partitionBy("route_id", "service_date")
    df_with_dups = df_parsed.withColumn("key_count", F.count("*").over(dup_window))

    # 3. Define failure flags
    invalid_condition = (
        F.col("route_id").isNull() | (F.col("route_id") == "") |
        F.col("route_name").isNull() | (F.col("route_name") == "") |
        F.col("parsed_date").isNull() |
        F.col("rides").isNull() | (F.col("rides") < 0) |
        (~F.col("day_type").isin(VALID_DAY_TYPES)) |
        (F.col("key_count") > 1)
    )

    # Assign rejection reason
    df_flagged = df_with_dups.withColumn(
        "rejection_reason",
        F.when(F.col("route_id").isNull() | (F.col("route_id") == ""), "Missing route_id")
        .when(F.col("route_name").isNull() | (F.col("route_name") == ""), "Missing route_name")
        .when(F.col("parsed_date").isNull(), "Invalid or null service_date")
        .when(F.col("rides").isNull(), "Null ride count")
        .when(F.col("rides") < 0, "Negative ride count")
        .when(~F.col("day_type").isin(VALID_DAY_TYPES), "Invalid day_type value")
        .when(F.col("key_count") > 1, "Duplicate route-date record")
        .otherwise("VALID")
    )

    quarantine_df = (
        df_flagged
        .filter(invalid_condition)
        .select("route_id", "route_name", "service_date", "day_type", "rides", "rejection_reason")
    )

    clean_df = (
        df_flagged
        .filter(~invalid_condition)
        .withColumn("service_date", F.col("parsed_date"))
        .select("route_id", "route_name", "service_date", "day_type", "rides")
    )

    # Persist quarantine if invalid rows are detected and path provided
    if quarantine_output_path is not None:
        q_count = quarantine_df.count()
        if q_count > 0:
            q_path = Path(quarantine_output_path)
            q_path.parent.mkdir(parents=True, exist_ok=True)
            # Export to single CSV for administrative review
            quarantine_df.toPandas().to_csv(q_path, index=False)

    return clean_df, quarantine_df
