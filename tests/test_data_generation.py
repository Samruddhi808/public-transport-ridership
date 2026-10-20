"""
Unit tests for synthetic data generation and reproducibility.
"""

import datetime
from scripts.generate_data import (
    ROUTES,
    classify_day_type,
    generate_ridership_data,
    get_us_holidays,
)


def test_data_generation_reproducibility():
    """Verify that identical seeds produce identical datasets."""
    start = datetime.date(2024, 1, 1)
    end = datetime.date(2024, 1, 3)

    records_1 = generate_ridership_data(start_date=start, end_date=end, seed=42)
    records_2 = generate_ridership_data(start_date=start, end_date=end, seed=42)

    assert len(records_1) == len(records_2)
    assert records_1 == records_2


def test_different_seeds_produce_different_data():
    """Verify that different seeds produce distinct ridership values."""
    start = datetime.date(2024, 1, 1)
    end = datetime.date(2024, 1, 5)

    records_a = generate_ridership_data(start_date=start, end_date=end, seed=42)
    records_b = generate_ridership_data(start_date=start, end_date=end, seed=999)

    rides_a = [r["rides"] for r in records_a]
    rides_b = [r["rides"] for r in records_b]
    assert rides_a != rides_b


def test_expected_record_count_and_columns():
    """Verify generated dataset length and required schema columns."""
    start = datetime.date(2024, 1, 1)
    end = datetime.date(2024, 1, 7)  # 7 days
    records = generate_ridership_data(start_date=start, end_date=end, seed=42)

    expected_count = 7 * len(ROUTES)
    assert len(records) == expected_count

    required_keys = {"route_id", "route_name", "service_date", "day_type", "rides"}
    for r in records:
        assert required_keys.issubset(r.keys())
        assert isinstance(r["rides"], int)
        assert r["rides"] >= 0


def test_unique_route_date_combinations():
    """Ensure that every (route_id, service_date) pair is strictly unique."""
    start = datetime.date(2024, 1, 1)
    end = datetime.date(2024, 1, 14)
    records = generate_ridership_data(start_date=start, end_date=end, seed=42)

    keys = [(r["route_id"], r["service_date"]) for r in records]
    assert len(keys) == len(set(keys)), "Duplicate route-date combinations detected!"


def test_holiday_and_day_type_classification():
    """Verify transit holiday rules and day type mapping."""
    holidays_2024 = get_us_holidays(2024)

    # New Year's Day (Jan 1, 2024 was a Monday holiday)
    new_years = datetime.date(2024, 1, 1)
    assert new_years in holidays_2024
    assert classify_day_type(new_years, holidays_2024) == "Sunday/Holiday"

    # Regular Tuesday (Jan 2, 2024)
    tue = datetime.date(2024, 1, 2)
    assert tue not in holidays_2024
    assert classify_day_type(tue, holidays_2024) == "Weekday"

    # Regular Saturday (Jan 6, 2024)
    sat = datetime.date(2024, 1, 6)
    assert classify_day_type(sat, holidays_2024) == "Saturday"

    # Regular Sunday (Jan 7, 2024)
    sun = datetime.date(2024, 1, 7)
    assert classify_day_type(sun, holidays_2024) == "Sunday/Holiday"
