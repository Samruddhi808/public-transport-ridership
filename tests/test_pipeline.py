"""
Integration tests for the PySpark data pipeline execution and output files.
"""

import json
from pathlib import Path
import pandas as pd
import pytest

from src.pipeline import run_pipeline


def test_pipeline_missing_file_raises_error(tmp_path):
    """Verify that pipeline raises FileNotFoundError if raw input does not exist."""
    fake_path = tmp_path / "non_existent.csv"
    with pytest.raises(FileNotFoundError):
        run_pipeline(raw_data_path=str(fake_path), processed_dir=str(tmp_path / "processed"))


def test_pipeline_processed_outputs_exist_and_valid():
    """Verify that all processed datasets exist, are non-empty, and have valid schema."""
    processed_dir = Path("data/processed")
    assert processed_dir.exists(), "Processed directory does not exist!"

    expected_files = [
        "route_summary.csv",
        "monthly_summary.csv",
        "day_type_summary.csv",
        "daily_summary.csv",
        "top_routes.csv",
        "bottom_routes.csv",
        "route_day_type_summary.csv",
        "route_monthly_heatmap.csv",
        "overall_metrics.json",
        "ridership.parquet",
    ]

    for filename in expected_files:
        filepath = processed_dir / filename
        assert filepath.exists(), f"Expected processed output '{filename}' is missing!"
        assert filepath.stat().st_size > 0, f"Processed output '{filename}' is empty!"

    # Verify overall_metrics.json content
    with open(processed_dir / "overall_metrics.json", "r", encoding="utf-8") as f:
        metrics = json.load(f)
    assert metrics["unique_routes"] == 20
    assert metrics["total_service_dates"] == 731
    assert metrics["total_rides"] > 0

    # Verify route_summary.csv schema
    route_df = pd.read_csv(processed_dir / "route_summary.csv")
    assert len(route_df) == 20
    assert "demand_rank" in route_df.columns
    assert "total_rides" in route_df.columns

    # Verify parquet file is readable
    parquet_df = pd.read_parquet(processed_dir / "ridership.parquet")
    assert len(parquet_df) == 14620
    assert "route_id" in parquet_df.columns
    assert "rides" in parquet_df.columns
