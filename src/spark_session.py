"""
SparkSession manager module for local PySpark processing.

Ensures proper environment configuration, local resource management,
Windows-specific network bindings, and clean session shutdown.
"""

import os
import sys
from typing import Optional
from pyspark.sql import SparkSession

# Ensure worker processes use the active Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

_spark_session: Optional[SparkSession] = None


def get_spark_session(
    app_name: str = "TransitPulse-RidershipAnalytics",
    master: str = "local[*]"
) -> SparkSession:
    """
    Get or create a configured SparkSession optimized for local Windows execution.

    Configured parameters:
    - master: local[*] for utilizing available local CPU cores
    - spark.driver.host / bindAddress: 127.0.0.1 to avoid Windows hostname/IPv6 stalls
    - spark.ui.enabled: false for faster startup and resource conservation
    - spark.sql.shuffle.partitions: 4 (appropriate for local data volumes)
    - spark.driver.memory: 2g
    """
    global _spark_session
    if _spark_session is not None and not _spark_session.sparkContext._jsc.sc().isStopped():
        return _spark_session

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.warehouse.dir", "file:///C:/Users/samruddhi/AppData/Local/Temp/spark-warehouse")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )

    _spark_session = builder.getOrCreate()
    _spark_session.sparkContext.setLogLevel("WARN")
    return _spark_session


def stop_spark_session() -> None:
    """Safely terminate the active SparkSession and release resources."""
    global _spark_session
    if _spark_session is not None:
        try:
            _spark_session.stop()
        except Exception:
            pass
        finally:
            _spark_session = None
