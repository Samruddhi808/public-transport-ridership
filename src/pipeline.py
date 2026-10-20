"""
Main Big Data ETL and PySpark processing pipeline.

Can be run directly via:
    python -m src.pipeline
"""

import json
import sys
import time
from pathlib import Path
from typing import Dict, Optional

from src.analytics import (
    compute_daily_system_summary,
    compute_day_type_summary,
    compute_monthly_summary,
    compute_overall_metrics,
    compute_route_day_type_summary,
    compute_route_monthly_heatmap_data,
    compute_route_summary,
    compute_top_and_bottom_routes,
    execute_spark_sql_queries,
)
from src.data_loader import load_raw_ridership, validate_and_clean_data
from src.spark_session import get_spark_session, stop_spark_session


def save_spark_df_to_csv(df, target_path: Path) -> None:
    """
    Export Spark DataFrame to a single, clean human-readable CSV file
    for reporting and Streamlit dashboard consumption.
    """
    target_path.parent.mkdir(parents=True, exist_ok=True)
    pandas_df = df.toPandas()
    pandas_df.to_csv(target_path, index=False)


def run_pipeline(
    raw_data_path: str = "data/raw/ridership.csv",
    processed_dir: str = "data/processed",
    app_name: str = "TransitPulse-Pipeline"
) -> Dict:
    """
    Execute the complete end-to-end Big Data Analytics pipeline.
    
    Returns:
        Dictionary of execution metrics and summary statistics.
    """
    raw_path = Path(raw_data_path)
    if not raw_path.exists():
        print(f"\n[ERROR] Raw ridership dataset not found at: {raw_path.resolve()}", file=sys.stderr)
        print("[ACTION REQUIRED] Generate the synthetic dataset first by running:", file=sys.stderr)
        print("    python scripts/generate_data.py\n", file=sys.stderr)
        raise FileNotFoundError(f"Input file missing: {raw_path}")

    proc_dir = Path(processed_dir)
    proc_dir.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    print("=" * 70)
    print("TRANSITPULSE: PUBLIC TRANSPORT RIDERSHIP PATTERN ANALYSIS")
    print("Apache PySpark Distributed Data Pipeline")
    print("=" * 70)
    print(f"[PIPELINE] Input data path:     {raw_path.resolve()}")
    print(f"[PIPELINE] Processed out dir:   {proc_dir.resolve()}")

    spark = None
    try:
        print("[PIPELINE] Initializing Apache PySpark session...")
        spark = get_spark_session(app_name=app_name)
        print(f"[PIPELINE] Spark session active: version {spark.version}")

        # 1. Ingestion & Validation
        print("[PIPELINE] Ingesting raw CSV via PySpark DataFrame API...")
        raw_df = load_raw_ridership(spark, raw_path)
        raw_count = raw_df.count()
        print(f"[PIPELINE] Raw rows ingested: {raw_count:,}")

        quarantine_file = proc_dir / "quarantined_records.csv"
        clean_df, quarantine_df = validate_and_clean_data(raw_df, quarantine_file)
        clean_count = clean_df.count()
        quarantine_count = quarantine_df.count()

        print(f"[PIPELINE] Data quality validation completed:")
        print(f"           - Clean records:       {clean_count:,}")
        print(f"           - Quarantined records:  {quarantine_count:,}")

        # Cache clean dataframe in memory for subsequent analytical transformations
        clean_df.cache()

        # 2. Overall Metrics
        print("[PIPELINE] Computing overall system metrics...")
        metrics = compute_overall_metrics(clean_df)

        # 3. Route-Level Summaries
        print("[PIPELINE] Computing route-level aggregations and rankings...")
        route_summary = compute_route_summary(clean_df)
        save_spark_df_to_csv(route_summary, proc_dir / "route_summary.csv")

        top_5, bottom_5 = compute_top_and_bottom_routes(clean_df, n=5)
        save_spark_df_to_csv(top_5, proc_dir / "top_routes.csv")
        save_spark_df_to_csv(bottom_5, proc_dir / "bottom_routes.csv")

        # 4. Time-Series Summaries
        print("[PIPELINE] Computing monthly trends and MoM growth...")
        monthly_summary = compute_monthly_summary(clean_df)
        save_spark_df_to_csv(monthly_summary, proc_dir / "monthly_summary.csv")

        # 5. Day-Type Distribution
        print("[PIPELINE] Computing operational day-type comparison...")
        day_type_summary = compute_day_type_summary(clean_df)
        save_spark_df_to_csv(day_type_summary, proc_dir / "day_type_summary.csv")

        # 6. Daily System Summary & Anomalies
        print("[PIPELINE] Computing daily totals, rolling 7-day average & anomalies...")
        daily_summary = compute_daily_system_summary(clean_df)
        save_spark_df_to_csv(daily_summary, proc_dir / "daily_summary.csv")

        # 7. Route by Day-Type Operational Breakdown
        print("[PIPELINE] Computing route by day-type breakdowns...")
        route_day_type = compute_route_day_type_summary(clean_df)
        save_spark_df_to_csv(route_day_type, proc_dir / "route_day_type_summary.csv")

        # 8. Heatmap Data
        print("[PIPELINE] Computing route monthly heatmap matrix...")
        heatmap_data = compute_route_monthly_heatmap_data(clean_df)
        save_spark_df_to_csv(heatmap_data, proc_dir / "route_monthly_heatmap.csv")

        # 9. Execute Spark SQL Demonstrations
        print("[PIPELINE] Running Spark SQL queries on temporary view...")
        sql_results = execute_spark_sql_queries(spark, clean_df)
        print("[PIPELINE] Spark SQL Query 1 (Route Rankings Top 3):")
        sql_results["sql_route_ranking"].show(3, truncate=False)
        print("[PIPELINE] Spark SQL Query 2 (Day Type Distribution):")
        sql_results["sql_day_type_breakdown"].show(truncate=False)

        # 10. Persist Columnar Parquet format for Big Data storage demonstration
        parquet_path = proc_dir / "ridership.parquet"
        print(f"[PIPELINE] Persisting clean dataset as Parquet: {parquet_path.resolve()}...")
        clean_df.toPandas().to_parquet(parquet_path, index=False)

        # Save KPI metrics as JSON
        with open(proc_dir / "overall_metrics.json", "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)

        elapsed = time.time() - start_time
        print("=" * 70)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 70)
        print(f"Total Passenger Rides:         {metrics['total_rides']:,}")
        print(f"Average Daily System Rides:    {metrics['avg_daily_system_rides']:,}")
        print(f"Monitored Routes:              {metrics['unique_routes']}")
        print(f"Total Service Dates:           {metrics['total_service_dates']}")
        print(f"Max Route-Day Ridership:       {metrics['max_route_day_rides']:,}")
        print(f"Min Route-Day Ridership:       {metrics['min_route_day_rides']:,}")
        print(f"Execution Duration:            {elapsed:.2f} seconds")
        print("=" * 70)
        print("[SUCCESS] All analytical datasets successfully generated and persisted.\n")

        return {
            "status": "SUCCESS",
            "metrics": metrics,
            "duration_seconds": elapsed,
            "records_processed": clean_count,
        }

    finally:
        print("[PIPELINE] Shutting down SparkSession cleanly...")
        stop_spark_session()
        print("[PIPELINE] SparkSession stopped.")


if __name__ == "__main__":
    run_pipeline()
