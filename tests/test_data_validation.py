"""
Unit tests for data validation, schema enforcement, and quarantining.
"""

import pytest
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.data_loader import load_raw_ridership, validate_and_clean_data
from src.spark_session import get_spark_session, stop_spark_session


@pytest.fixture(scope="module")
def spark():
    """Module-level SparkSession fixture."""
    session = get_spark_session(app_name="TransitPulse-ValidationTests")
    yield session
    # Don't stop session here to allow other test modules to share it cleanly,
    # or stop at end of session.


def test_missing_source_file_raises_error(spark):
    """Ensure FileNotFoundError is raised when raw CSV does not exist."""
    with pytest.raises(FileNotFoundError) as exc_info:
        load_raw_ridership(spark, "non_existent_folder/missing_ridership.csv")
    assert "Raw ridership dataset not found" in str(exc_info.value)


def test_data_validation_quarantines_corrupted_rows(spark):
    """Verify that nulls, negative values, and invalid day types are quarantined."""
    test_schema = StructType([
        StructField("route_id", StringType(), True),
        StructField("route_name", StringType(), True),
        StructField("service_date", StringType(), True),
        StructField("day_type", StringType(), True),
        StructField("rides", IntegerType(), True),
    ])

    test_data = [
        # 1. Clean valid row
        ("R101", "101 Metro Loop", "2024-01-01", "Sunday/Holiday", 2500),
        # 2. Corrupted: null route_id
        (None, "102 Harbor Express", "2024-01-01", "Sunday/Holiday", 2200),
        # 3. Corrupted: negative rides
        ("R103", "103 University", "2024-01-01", "Sunday/Holiday", -50),
        # 4. Corrupted: invalid day_type
        ("R104", "104 Crosstown", "2024-01-01", "Funday", 1800),
        # 5. Corrupted: unparseable date
        ("R105", "105 Airport", "not-a-date", "Weekday", 1500),
        # 6 & 7. Corrupted: duplicate (route_id, service_date)
        ("R106", "106 Tech", "2024-01-02", "Weekday", 1200),
        ("R106", "106 Tech", "2024-01-02", "Weekday", 1250),
    ]

    raw_df = spark.createDataFrame(test_data, schema=test_schema)
    clean_df, quarantine_df = validate_and_clean_data(raw_df)

    clean_rows = clean_df.collect()
    quarantine_rows = quarantine_df.collect()

    assert len(clean_rows) == 1
    assert clean_rows[0]["route_id"] == "R101"
    assert clean_rows[0]["rides"] == 2500

    # 6 invalid records (1 null route + 1 negative + 1 invalid day + 1 bad date + 2 duplicate entries)
    assert len(quarantine_rows) == 6

    reasons = [r["rejection_reason"] for r in quarantine_rows]
    assert "Missing route_id" in reasons
    assert "Negative ride count" in reasons
    assert "Invalid day_type value" in reasons
    assert "Invalid or null service_date" in reasons
    assert "Duplicate route-date record" in reasons
