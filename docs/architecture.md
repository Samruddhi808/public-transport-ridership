# System Architecture & Technical Design

**Project Title:** Public Transport Ridership Pattern Analysis Using Apache PySpark  
**System Name:** TransitPulse  
**Technology Stack:** Python 3.11, Apache PySpark 4.0.4, Spark SQL, Apache Parquet, Streamlit 1.66, Plotly, Pytest  

---

## 1. High-Level Architecture Overview

TransitPulse implements an end-to-end Big Data Analytics architecture divided into five modular layers:

1. **Synthetic Data Generation Layer:** Deterministic simulation of multi-year transit operations.
2. **PySpark Data Ingestion & Validation Layer:** Schema enforcement, null checking, anomaly quarantining, and key validation.
3. **Distributed Analytical Engine:** Distributed DataFrame aggregations, Window functions (ranking, rolling averages, lead/lag), and Spark SQL temporary views.
4. **Persistence & Columnar Storage Layer:** Dual-storage strategy persisting human-readable summary CSVs alongside high-performance Apache Parquet columnar storage.
5. **Interactive Visualization Layer:** Streamlit dashboard powered by Plotly for dynamic filtering, KPI calculation, and pattern discovery.

```mermaid
flowchart TD
    subgraph DataGen [Synthetic Data Generation]
        G[scripts/generate_data.py] -->|Generates 14,620 Records| RAW[(data/raw/ridership.csv)]
    end

    subgraph SparkPipeline [Apache PySpark Distributed Pipeline]
        RAW --> INGEST[src/data_loader.py: Schema Ingestion]
        INGEST --> VALIDATE{Data Quality Validation}
        
        VALIDATE -->|Corrupted / Duplicate Rows| QUAR[(data/processed/quarantined_records.csv)]
        VALIDATE -->|Clean Ingested Data| CLEAN[clean_df: Cache in Memory]
        
        CLEAN --> OVERALL[compute_overall_metrics]
        CLEAN --> ROUTE[compute_route_summary: Window DENSE_RANK]
        CLEAN --> TIME[compute_monthly_summary: Window LAG MoM]
        CLEAN --> DAYTYPE[compute_day_type_summary: Share %]
        CLEAN --> DAILY[compute_daily_system_summary: Rolling 7D Window & Anomaly Flags]
        CLEAN --> HEATMAP[compute_route_monthly_heatmap_data]
        CLEAN --> SQL[Spark SQL Temporary View 'ridership']
    end

    subgraph Storage [Storage & Outputs]
        OVERALL --> JSON_OUT[(overall_metrics.json)]
        ROUTE --> ROUTE_CSV[(route_summary.csv / top_routes.csv)]
        TIME --> MONTH_CSV[(monthly_summary.csv)]
        DAYTYPE --> DAY_CSV[(day_type_summary.csv)]
        DAILY --> DAILY_CSV[(daily_summary.csv)]
        HEATMAP --> HEAT_CSV[(route_monthly_heatmap.csv)]
        CLEAN --> PARQUET[(ridership.parquet)]
    end

    subgraph Dashboard [Interactive Streamlit UI]
        Storage --> CACHE[@st.cache_data Precomputed Loader]
        CACHE --> FILTERS[Sidebar Filters: Dates, Routes, Day Types]
        FILTERS --> KPIS[KPI Metric Cards]
        FILTERS --> CHARTS[Plotly Visualizations & Heatmap]
        FILTERS --> EXPORT[CSV Data Download]
        FILTERS --> INSIGHTS[Dynamic Rule-Based Insights]
    end
```

---

## 2. PySpark Data Processing Pipeline Detail

### 2.1 Schema Definition & Ingestion
The raw CSV is parsed using an explicit `StructType` rather than inferring schemas at runtime:
```python
RAW_SCHEMA = StructType([
    StructField("route_id", StringType(), False),
    StructField("route_name", StringType(), False),
    StructField("service_date", StringType(), False),
    StructField("day_type", StringType(), False),
    StructField("rides", IntegerType(), False),
])
```

### 2.2 Quality Assurance & Quarantine Protocol
Data records undergo multi-factor validation before entering the analytical graph:
- **Null / Empty Detection:** Rejects rows missing mandatory route identifiers or names.
- **ISO-8601 Date Parsing Guard:** Uses regex pattern checking (`^\d{4}-\d{2}-\d{2}$`) and Spark `to_date` to prevent unparseable timestamps from aborting the distributed job.
- **Domain Constraints:** Rejects negative ride values (`rides < 0`) and unapproved day-type strings.
- **Uniqueness Check:** Evaluates window partition count over `(route_id, service_date)` to isolate accidental duplications.

Records failing any test are tagged with an explicit `rejection_reason` and routed to `data/processed/quarantined_records.csv`.

---

## 3. Distributed Analytical Window Operations

### 3.1 Dense Route Ranking
Calculates the operational demand rank of bus routes across total passenger volume:
```sql
SELECT route_id, route_name, SUM(rides) AS total_passenger_rides,
       DENSE_RANK() OVER (ORDER BY SUM(rides) DESC) AS rank
FROM ridership
GROUP BY route_id, route_name
```

### 3.2 Month-Over-Month (MoM) Growth using SQL LAG
Evaluates seasonal progression and ridership trends:
```sql
WITH monthly AS (
    SELECT YEAR(service_date) yr, MONTH(service_date) mo,
           DATE_FORMAT(service_date, 'yyyy-MM') ym, SUM(rides) tot
    FROM ridership GROUP BY 1, 2, 3
)
SELECT ym, tot, LAG(tot, 1) OVER (ORDER BY yr, mo) AS prev_month,
       ROUND(((tot - LAG(tot, 1) OVER (ORDER BY yr, mo)) * 100.0) / 
             LAG(tot, 1) OVER (ORDER BY yr, mo), 2) AS mom_growth_pct
FROM monthly ORDER BY yr, mo
```

### 3.3 Rolling 7-Day Trendline & Anomaly Detection
Uses physical window framing:
```python
time_window = Window.orderBy("service_date").rowsBetween(-6, 0)
daily_rolling = daily_base.withColumn("rolling_7d_avg", F.round(F.avg("daily_total_rides").over(time_window), 2))
```
Statistical anomaly boundaries are computed across system-wide mean and standard deviation:
$$\text{Threshold}_{\text{High}} = \mu + 2\sigma, \quad \text{Threshold}_{\text{Low}} = \max(0, \mu - 2\sigma)$$

---

## 4. Execution Flow & Resource Management

- **Local Cluster Simulation:** Spark operates under `.master("local[*]")`, spawning worker threads that utilize all CPU cores without external cluster infrastructure.
- **Windows Socket Optimization:** Sets `spark.driver.host` and `spark.driver.bindAddress` to `127.0.0.1` to prevent Windows IPv6 resolution timeouts.
- **Clean Teardown:** Spark contexts are encapsulated in `try...finally` blocks to guarantee JVM release upon pipeline completion.
- **Client Responsiveness:** The Streamlit dashboard decodes precomputed CSV summaries and Parquet data into memory via `@st.cache_data`, ensuring instant sub-second filtering without repeatedly spinning up a heavyweight JVM on every slider click.
