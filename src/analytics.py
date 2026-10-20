"""
Analytical transformations and Spark SQL query engine for ridership data.

Provides distributed aggregations, window operations, time-series metrics,
operational pattern breakdowns, and anomaly detection.
"""

from typing import Any, Dict, List, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def compute_overall_metrics(df: DataFrame) -> Dict[str, Any]:
    """
    Compute system-wide summary key performance indicators (KPIs).
    
    Returns:
        Dictionary containing total rides, average daily system rides,
        unique route count, total service days, max route-day rides,
        and min route-day rides.
    """
    agg_row = df.select(
        F.sum("rides").alias("total_rides"),
        F.countDistinct("route_id").alias("unique_routes"),
        F.countDistinct("service_date").alias("total_service_dates"),
        F.max("rides").alias("max_route_day_rides"),
        F.min("rides").alias("min_route_day_rides"),
        F.avg("rides").alias("avg_route_day_rides")
    ).collect()[0]

    total_rides = int(agg_row["total_rides"] or 0)
    service_dates = int(agg_row["total_service_dates"] or 1)
    avg_daily_system = round(total_rides / service_dates, 2) if service_dates > 0 else 0.0

    return {
        "total_rides": total_rides,
        "avg_daily_system_rides": avg_daily_system,
        "unique_routes": int(agg_row["unique_routes"] or 0),
        "total_service_dates": service_dates,
        "max_route_day_rides": int(agg_row["max_route_day_rides"] or 0),
        "min_route_day_rides": int(agg_row["min_route_day_rides"] or 0),
        "avg_route_day_rides": round(float(agg_row["avg_route_day_rides"] or 0.0), 2),
    }


def compute_route_summary(df: DataFrame) -> DataFrame:
    """
    Compute comprehensive per-route aggregations and demand rankings.
    """
    route_agg = (
        df.groupBy("route_id", "route_name")
        .agg(
            F.sum("rides").alias("total_rides"),
            F.round(F.avg("rides"), 2).alias("avg_daily_rides"),
            F.min("rides").alias("min_rides"),
            F.max("rides").alias("max_rides"),
            F.count("service_date").alias("service_days")
        )
    )

    # Calculate dense rank ordered by total rides descending
    rank_window = Window.orderBy(F.col("total_rides").desc())
    route_ranked = route_agg.withColumn("demand_rank", F.dense_rank().over(rank_window))
    return route_ranked.sort("demand_rank")


def compute_monthly_summary(df: DataFrame) -> DataFrame:
    """
    Compute monthly ridership totals, average daily ridership per month,
    and month-over-month (MoM) growth percentage using Window LAG.
    """
    df_with_ym = (
        df
        .withColumn("year", F.year("service_date"))
        .withColumn("month", F.month("service_date"))
        .withColumn("year_month", F.date_format("service_date", "yyyy-MM"))
    )

    monthly_base = (
        df_with_ym
        .groupBy("year", "month", "year_month")
        .agg(
            F.sum("rides").alias("total_rides"),
            F.countDistinct("service_date").alias("days_in_month"),
            F.countDistinct("route_id").alias("active_routes")
        )
        .withColumn(
            "avg_daily_rides",
            F.round(F.col("total_rides") / F.col("days_in_month"), 2)
        )
    )

    # Window ordered chronologically by year and month
    chron_window = Window.orderBy("year", "month")
    monthly_summary = (
        monthly_base
        .withColumn("prev_month_rides", F.lag("total_rides", 1).over(chron_window))
        .withColumn(
            "mom_growth_pct",
            F.when(
                F.col("prev_month_rides").isNotNull(),
                F.round(((F.col("total_rides") - F.col("prev_month_rides")) / F.col("prev_month_rides")) * 100, 2)
            ).otherwise(None)
        )
        .sort("year", "month")
    )

    return monthly_summary


def compute_day_type_summary(df: DataFrame) -> DataFrame:
    """
    Compare demand distribution across Weekday, Saturday, and Sunday/Holiday.
    Calculates total rides, distinct service dates, average daily rides,
    and system ridership percentage share.
    """
    total_system_rides = df.select(F.sum("rides")).collect()[0][0] or 1

    day_type_agg = (
        df.groupBy("day_type")
        .agg(
            F.sum("rides").alias("total_rides"),
            F.countDistinct("service_date").alias("total_days"),
            F.round(F.avg("rides"), 2).alias("avg_route_rides")
        )
        .withColumn(
            "avg_daily_system_rides",
            F.round(F.col("total_rides") / F.col("total_days"), 2)
        )
        .withColumn(
            "ridership_share_pct",
            F.round((F.col("total_rides") / F.lit(total_system_rides)) * 100, 2)
        )
        .sort(F.col("total_rides").desc())
    )

    return day_type_agg


def compute_daily_system_summary(df: DataFrame) -> DataFrame:
    """
    Aggregate daily system-wide ridership, compute a 7-day rolling average,
    and flag statistical anomalies (> 2 standard deviations from mean).
    """
    daily_base = (
        df.groupBy("service_date", "day_type")
        .agg(F.sum("rides").alias("daily_total_rides"))
        .sort("service_date")
    )

    # 7-day rolling average window (current row plus previous 6 rows)
    time_window = Window.orderBy("service_date").rowsBetween(-6, 0)
    daily_rolling = daily_base.withColumn(
        "rolling_7d_avg",
        F.round(F.avg("daily_total_rides").over(time_window), 2)
    )

    # Compute overall mean and standard deviation for anomaly detection
    stats = daily_base.select(
        F.avg("daily_total_rides").alias("mean"),
        F.stddev("daily_total_rides").alias("std")
    ).collect()[0]

    mean_val = float(stats["mean"] or 0.0)
    std_val = float(stats["std"] or 0.0)
    high_threshold = mean_val + (2.0 * std_val)
    low_threshold = max(0.0, mean_val - (2.0 * std_val))

    daily_anomalies = daily_rolling.withColumn(
        "is_high_anomaly",
        F.when(F.col("daily_total_rides") > high_threshold, True).otherwise(False)
    ).withColumn(
        "is_low_anomaly",
        F.when(F.col("daily_total_rides") < low_threshold, True).otherwise(False)
    )

    return daily_anomalies


def compute_top_and_bottom_routes(
    df: DataFrame,
    n: int = 5
) -> Tuple[DataFrame, DataFrame]:
    """
    Extract top N highest-demand routes and bottom N routes by total ridership.
    """
    route_summary = compute_route_summary(df)
    top_routes = route_summary.limit(n)
    bottom_routes = route_summary.sort("total_rides").limit(n)
    return top_routes, bottom_routes


def compute_route_day_type_summary(df: DataFrame) -> DataFrame:
    """
    Analyze operational patterns across route and day-type combinations.
    Shows route responsiveness to weekend service.
    """
    return (
        df.groupBy("route_id", "route_name", "day_type")
        .agg(
            F.sum("rides").alias("total_rides"),
            F.round(F.avg("rides"), 2).alias("avg_daily_rides"),
            F.count("service_date").alias("days_observed")
        )
        .sort("route_id", "day_type")
    )


def compute_route_monthly_heatmap_data(df: DataFrame) -> DataFrame:
    """
    Pivot route ridership by month for heatmap visualization.
    """
    df_ym = df.withColumn("year_month", F.date_format("service_date", "yyyy-MM"))
    return (
        df_ym.groupBy("route_name", "year_month")
        .agg(F.sum("rides").alias("monthly_rides"))
        .sort("route_name", "year_month")
    )


def execute_spark_sql_queries(spark: SparkSession, df: DataFrame) -> Dict[str, DataFrame]:
    """
    Execute demonstrated Spark SQL queries on temporary view 'ridership'.
    
    These queries showcase:
    1. Window ranking functions (DENSE_RANK)
    2. Analytical window functions (LAG, OVER())
    3. Conditional aggregations with CASE statements
    4. Subqueries and Common Table Expressions (CTEs)
    """
    df.createOrReplaceTempView("ridership")

    queries = {
        # Query 1: Route Rankings using Spark SQL Window Function
        "sql_route_ranking": spark.sql("""
            SELECT 
                route_id,
                route_name,
                SUM(rides) AS total_passenger_rides,
                ROUND(AVG(rides), 1) AS avg_rides_per_day,
                DENSE_RANK() OVER (ORDER BY SUM(rides) DESC) AS rank
            FROM ridership
            GROUP BY route_id, route_name
            ORDER BY rank ASC
        """),

        # Query 2: Day Type Comparison with Global Percentage Share
        "sql_day_type_breakdown": spark.sql("""
            SELECT
                day_type,
                COUNT(DISTINCT service_date) AS distinct_service_days,
                SUM(rides) AS total_rides,
                ROUND(AVG(rides), 1) AS avg_rides_per_route_day,
                ROUND(SUM(rides) * 100.0 / SUM(SUM(rides)) OVER(), 2) AS percentage_share
            FROM ridership
            GROUP BY day_type
            ORDER BY total_rides DESC
        """),

        # Query 3: Monthly Growth Progression using SQL LAG
        "sql_monthly_growth": spark.sql("""
            WITH monthly_totals AS (
                SELECT
                    YEAR(service_date) AS yr,
                    MONTH(service_date) AS mo,
                    DATE_FORMAT(service_date, 'yyyy-MM') AS yyyy_mm,
                    SUM(rides) AS month_rides
                FROM ridership
                GROUP BY YEAR(service_date), MONTH(service_date), DATE_FORMAT(service_date, 'yyyy-MM')
            )
            SELECT
                yyyy_mm,
                month_rides,
                LAG(month_rides, 1) OVER (ORDER BY yr, mo) AS prev_month_rides,
                ROUND(((month_rides - LAG(month_rides, 1) OVER (ORDER BY yr, mo)) * 100.0) / 
                      LAG(month_rides, 1) OVER (ORDER BY yr, mo), 2) AS mom_growth_pct
            FROM monthly_totals
            ORDER BY yr, mo
        """),

        # Query 4: Top 10 Single-Day Route Peak Ridership Events
        "sql_top_peak_events": spark.sql("""
            SELECT
                service_date,
                route_id,
                route_name,
                day_type,
                rides
            FROM ridership
            ORDER BY rides DESC
            LIMIT 10
        """)
    }

    return queries
