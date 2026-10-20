"""
Synthetic Public Transport Ridership Dataset Generator.

ACADEMIC DISCLAIMER:
This script produces a synthetic, simulated dataset for academic demonstration
and educational big data analytics purposes only. The records are artificial and
do not represent actual operational records of the Chicago Transit Authority (CTA)
or any other public transit agency.

Dataset Details:
- 2 Calendar Years: 2024 (leap year, 366 days) and 2025 (365 days) -> 731 days
- 20 Fictional Bus Routes
- Total Records: 14,620 route-date observations
- Output Columns: route_id, route_name, service_date, day_type, rides
"""

import argparse
import csv
import datetime
import math
import random
from pathlib import Path
from typing import Dict, List, Set, Tuple

# 20 Fictional Bus Routes with Route Type and Baseline Daily Demand
ROUTES = [
    {"id": "R101", "name": "101 Metro Loop Trunk", "base": 4800, "volatility": 0.10, "type": "Trunk"},
    {"id": "R102", "name": "102 Harbor Express", "base": 4200, "volatility": 0.12, "type": "Express"},
    {"id": "R103", "name": "103 University Connector", "base": 3900, "volatility": 0.15, "type": "University"},
    {"id": "R104", "name": "104 Crosstown Parkway", "base": 3600, "volatility": 0.11, "type": "Trunk"},
    {"id": "R105", "name": "105 Airport Shuttle Direct", "base": 3400, "volatility": 0.14, "type": "Shuttle"},
    {"id": "R106", "name": "106 Tech Corridor Flyer", "base": 3200, "volatility": 0.13, "type": "Commuter"},
    {"id": "R107", "name": "107 Riverside Central Line", "base": 2900, "volatility": 0.12, "type": "Local"},
    {"id": "R108", "name": "108 Southside Commuter", "base": 2700, "volatility": 0.12, "type": "Commuter"},
    {"id": "R109", "name": "109 North Hills Local", "base": 2400, "volatility": 0.13, "type": "Local"},
    {"id": "R110", "name": "110 Civic Center Circulator", "base": 2200, "volatility": 0.14, "type": "Circulator"},
    {"id": "R111", "name": "111 Bayview Rapid Transit", "base": 2000, "volatility": 0.13, "type": "Rapid"},
    {"id": "R112", "name": "112 Greenway Community Local", "base": 1800, "volatility": 0.14, "type": "Feeder"},
    {"id": "R113", "name": "113 Central Avenue Shuttle", "base": 1650, "volatility": 0.15, "type": "Shuttle"},
    {"id": "R114", "name": "114 West End Direct", "base": 1500, "volatility": 0.15, "type": "Local"},
    {"id": "R115", "name": "115 Lakeside Parkway Express", "base": 1350, "volatility": 0.16, "type": "Express"},
    {"id": "R116", "name": "116 Foothill Transit Feeder", "base": 1200, "volatility": 0.16, "type": "Feeder"},
    {"id": "R117", "name": "117 Pine Street Neighborhood", "base": 1050, "volatility": 0.17, "type": "Feeder"},
    {"id": "R118", "name": "118 Valley Industrial Link", "base": 950, "volatility": 0.18, "type": "Industrial"},
    {"id": "R119", "name": "119 Riverfront Heritage Loop", "base": 850, "volatility": 0.19, "type": "Circulator"},
    {"id": "R120", "name": "120 Grand Boulevard Crosstown", "base": 750, "volatility": 0.20, "type": "Feeder"},
]

# Monthly seasonal demand multipliers (Jan=1 ... Dec=12)
MONTHLY_MULTIPLIERS = {
    1: 0.90,   # Winter slow period
    2: 0.92,   # Cold weather
    3: 1.02,   # Spring resumption
    4: 1.04,   # Mild weather
    5: 1.06,   # Pre-summer activities
    6: 1.08,   # Summer peak start
    7: 1.07,   # Summer travel
    8: 1.05,   # Late summer
    9: 1.12,   # School/University return surge
    10: 1.10,  # Peak autumn commuting
    11: 1.01,  # Thanksgiving seasonal transition
    12: 0.95,  # Holiday recess
}


def get_us_holidays(year: int) -> Set[datetime.date]:
    """Return recognized federal public transport holiday dates for a given year."""
    holidays = set()

    # Fixed-date holidays
    holidays.add(datetime.date(year, 1, 1))    # New Year's Day
    holidays.add(datetime.date(year, 6, 19))   # Juneteenth
    holidays.add(datetime.date(year, 7, 4))    # Independence Day
    holidays.add(datetime.date(year, 12, 25))  # Christmas Day

    # Floating holidays:
    # MLK Day: 3rd Monday in January
    jan_mondays = [datetime.date(year, 1, d) for d in range(1, 32) if datetime.date(year, 1, d).weekday() == 0]
    holidays.add(jan_mondays[2])

    # Memorial Day: Last Monday in May
    may_mondays = [datetime.date(year, 5, d) for d in range(1, 32) if datetime.date(year, 5, d).weekday() == 0]
    holidays.add(may_mondays[-1])

    # Labor Day: First Monday in September
    sep_mondays = [datetime.date(year, 9, d) for d in range(1, 31) if datetime.date(year, 9, d).weekday() == 0]
    holidays.add(sep_mondays[0])

    # Thanksgiving: Fourth Thursday in November
    nov_thursdays = [datetime.date(year, 11, d) for d in range(1, 31) if datetime.date(year, 11, d).weekday() == 3]
    holidays.add(nov_thursdays[3])

    return holidays


def classify_day_type(current_date: datetime.date, holidays: Set[datetime.date]) -> str:
    """
    Classify date into Weekday, Saturday, or Sunday/Holiday.
    Standard public transit agencies operate Sunday/Holiday schedules on public holidays.
    """
    if current_date in holidays or current_date.weekday() == 6:
        return "Sunday/Holiday"
    elif current_date.weekday() == 5:
        return "Saturday"
    else:
        return "Weekday"


def calculate_daily_rides(
    route: Dict,
    current_date: datetime.date,
    day_type: str,
    rng: random.Random
) -> int:
    """
    Calculate deterministic passenger ridership for a specific route and service date.
    
    Factors considered:
    1. Base route demand
    2. Day-of-week / Day-type scaling factor
    3. Monthly seasonal scaling factor
    4. Year-over-year growth (2025 ~3.5% higher than 2024)
    5. Route-type specific behavior (e.g. University route drops in summer)
    6. Controlled Gaussian random noise and occasional special events
    """
    base = route["base"]
    volatility = route["volatility"]
    route_type = route["type"]

    # 1. Day-type multiplier
    if day_type == "Weekday":
        # Slight variation between Tuesday-Thursday peaks vs Monday/Friday
        weekday_idx = current_date.weekday()
        if weekday_idx in (1, 2, 3):  # Tue, Wed, Thu
            day_mult = 1.02
        else:  # Mon, Fri
            day_mult = 0.98
    elif day_type == "Saturday":
        day_mult = 0.68
    else:  # Sunday/Holiday
        day_mult = 0.48

    # 2. Monthly seasonal multiplier
    month_mult = MONTHLY_MULTIPLIERS[current_date.month]

    # 3. Route-specific adjustments
    if route_type == "University":
        # Major drops during Summer break (June, July, August) and Winter break (late Dec)
        if current_date.month in (6, 7):
            month_mult *= 0.55
        elif current_date.month == 8:
            month_mult *= 0.70
        elif current_date.month == 12 and current_date.day > 20:
            month_mult *= 0.50
        elif current_date.month in (9, 10):
            month_mult *= 1.25  # Fall term rush

    elif route_type == "Commuter":
        # Commuter routes drop heavily on weekends
        if day_type == "Saturday":
            day_mult = 0.45
        elif day_type == "Sunday/Holiday":
            day_mult = 0.28

    elif route_type in ("Express", "Shuttle"):
        if current_date.month in (6, 7, 8):
            month_mult *= 1.15  # Tourism/Airport boost

    # 4. Year-over-year expansion factor (2025 represents +3.8% system growth)
    year_mult = 1.0 if current_date.year == 2024 else 1.038

    # 5. Controlled random variation
    noise = rng.gauss(0, volatility)

    # 6. Occasional macro events:
    # Rare high-demand event (e.g., city marathon, convention, downtown festival) ~ 1.5% chance
    event_mult = 1.0
    roll = rng.random()
    if roll < 0.012:
        event_mult = 1.25  # Big event bump
    elif roll > 0.990:
        event_mult = 0.75  # Severe storm / weather disruption

    # Compute expected rides
    expected = base * day_mult * month_mult * year_mult * (1.0 + noise) * event_mult
    rides = max(0, int(round(expected)))
    return rides


def generate_ridership_data(
    start_date: datetime.date = datetime.date(2024, 1, 1),
    end_date: datetime.date = datetime.date(2025, 12, 31),
    seed: int = 42
) -> List[Dict]:
    """
    Generate synthetic ridership dataset for all defined routes and date range.
    Returns list of dicts with guaranteed unique (route_id, service_date) records.
    """
    rng = random.Random(seed)
    
    # Collect holidays for all years in range
    years = range(start_date.year, end_date.year + 1)
    holidays: Set[datetime.date] = set()
    for y in years:
        holidays.update(get_us_holidays(y))

    records: List[Dict] = []
    current_date = start_date
    one_day = datetime.timedelta(days=1)

    while current_date <= end_date:
        day_type = classify_day_type(current_date, holidays)
        iso_date = current_date.isoformat()

        for route in ROUTES:
            rides = calculate_daily_rides(route, current_date, day_type, rng)
            records.append({
                "route_id": route["id"],
                "route_name": route["name"],
                "service_date": iso_date,
                "day_type": day_type,
                "rides": rides,
            })

        current_date += one_day

    return records


def save_csv(records: List[Dict], filepath: Path) -> None:
    """Save records to CSV with exact field order."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["route_id", "route_name", "service_date", "day_type", "rides"]
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic public transport ridership dataset.")
    parser.add_argument("--output", type=str, default="data/raw/ridership.csv", help="Output raw CSV path")
    parser.add_argument("--sample", type=str, default="data/sample/ridership_sample.csv", help="Sample CSV path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--start-year", type=int, default=2024, help="Start year")
    parser.add_argument("--end-year", type=int, default=2025, help="End year")
    args = parser.parse_args()

    start_date = datetime.date(args.start_year, 1, 1)
    end_date = datetime.date(args.end_year, 12, 31)

    print(f"[DATA GENERATOR] Generating ridership data from {start_date} to {end_date} (Seed: {args.seed})...")
    records = generate_ridership_data(start_date, end_date, seed=args.seed)
    
    total_days = (end_date - start_date).days + 1
    expected_count = total_days * len(ROUTES)

    print(f"[DATA GENERATOR] Generated {len(records)} records across {len(ROUTES)} routes for {total_days} days.")
    assert len(records) == expected_count, f"Expected {expected_count} rows, got {len(records)}"

    out_path = Path(args.output)
    save_csv(records, out_path)
    print(f"[DATA GENERATOR] Saved raw dataset to: {out_path.resolve()}")

    sample_path = Path(args.sample)
    save_csv(records[:50], sample_path)
    print(f"[DATA GENERATOR] Saved sample preview (50 rows) to: {sample_path.resolve()}")


if __name__ == "__main__":
    main()
