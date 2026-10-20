# Academic Mini-Project Report

## Public Transport Ridership Pattern Analysis Using Apache PySpark

**Department of Computer Science and Engineering / Information Technology**  
**Course:** Big Data Technologies & Distributed Computing  
**Project Name:** TransitPulse  
**Target Platform:** Windows 10/11, Python 3.11, Apache PySpark 4.0.4  

---

### Abstract
Urban public transport agencies face complex operational challenges in matching transit vehicle capacity to dynamic commuter demand patterns. Analyzing multi-year, multi-route transit demand requires scalable distributed data processing techniques capable of aggregating large volumes without driver memory bottlenecks. This college mini-project presents **TransitPulse**, an end-to-end Big Data Analytics application developed using **Apache PySpark 4.0.4**, **Spark SQL**, and **Streamlit**. A deterministic synthetic dataset representing 20 fictional bus corridors over two complete calendar years (2024–2025; 14,620 observations) was modeled with realistic seasonal, day-type, route-tier, and holiday characteristics. A distributed PySpark pipeline ingests raw records with an explicit schema, performs multi-criteria data quality validation and quarantining, and executes advanced window transformations including dense ranking, 7-day rolling averages, and month-over-month growth calculations. Analytical outputs are persisted in dual formats (human-readable CSV summaries and columnar Apache Parquet). An interactive Streamlit dashboard provides dynamic multi-attribute filtering, KPI tracking, and Plotly visualizations. Automated testing with Pytest achieved a 100% pass rate across 14 unit and integration test fixtures.

---

### 1. Introduction
Modern urban mobility systems generate vast volumes of operational ridership records. Traditional relational database management systems (RDBMS) and single-threaded Python scripts (e.g., standard pandas in-memory dataframes) struggle to scale when ingesting multi-year automated passenger counter (APC) datasets. Distributed data computing frameworks such as Apache Spark provide high-throughput distributed memory abstractions that decouple storage from computation. This project demonstrates how big data engineering principles—such as schema enforcement, lazy evaluation, directed acyclic graph (DAG) optimization, window transformations, and columnar storage—can be applied to solve realistic public transit planning problems.

---

### 2. Problem Statement
Public transport authorities frequently experience service inefficiencies such as:
1. Under-allocation of fleet capacity along high-density trunk corridors during weekday rush periods.
2. Over-scheduling of high-capacity buses on low-demand suburban or feeder routes during weekends and holidays.
3. Inability to rapidly detect statistical ridership anomalies resulting from localized special events or severe weather disruptions.
4. Legacy batch reporting systems that take hours to compute basic route-by-month and year-over-year ridership changes.

The challenge is to engineer a reliable, distributed, offline data pipeline that automates the ingestion, validation, analytical processing, and interactive visualization of transit patterns.

---

### 3. Objectives
The key objectives of this project are:
- **Synthetic Data Simulation:** Develop a mathematically deterministic generator producing two full calendar years (2024 and 2025) of daily transit ridership across 20 distinct bus routes (14,620 records).
- **Distributed Ingestion & Quality Control:** Implement PySpark data loading with strict schema enforcement, non-negative checks, date parsing, and an automated quarantine mechanism for invalid rows.
- **Advanced PySpark & Spark SQL Analytics:** Perform distributed aggregations, compute dense route demand rankings, 7-day rolling averages, month-over-month (MoM) growth, and day-type shares.
- **Dual-Storage Persistence:** Output aggregated summary tables in CSV format and clean analytical datasets in Apache Parquet format.
- **Interactive Visualization:** Build an interactive, responsive Streamlit dashboard with KPI metric cards, Plotly charts, heatmaps, and downloadable CSV exports.
- **Comprehensive Quality Assurance:** Verify pipeline robustness through automated unit and integration tests using Pytest.

---

### 4. Dataset Design & Characteristics
To ensure academic rigor without violating privacy or transit agency confidentiality, a synthetic dataset was modeled with the following explicit attributes:

- **Temporal Coverage:** January 1, 2024 to December 31, 2025 (731 days, accurately including February 29 for the 2024 leap year).
- **Route Diversity:** 20 fictional routes categorized into functional tiers: Trunk, Express, University, Commuter, Shuttle, Local, Circulator, and Feeder.
- **Total Record Count:** $731 \text{ days} \times 20 \text{ routes} = 14,620 \text{ observations}$.
- **Schema:**
  - `route_id`: String (e.g., "R101", "R102", ..., "R120")
  - `route_name`: String (e.g., "101 Metro Loop Trunk")
  - `service_date`: ISO-8601 Date (`YYYY-MM-DD`)
  - `day_type`: Categorical (`Weekday`, `Saturday`, `Sunday/Holiday`)
  - `rides`: Non-negative Integer

#### Mathematical Simulation Dynamics
Daily route ridership was computed using deterministic multi-factor scaling:
$$Rides = \text{round}\left( \text{Base} \times M_{\text{day}} \times M_{\text{month}} \times M_{\text{year}} \times (1 + \epsilon) \times M_{\text{event}} \right)$$
where:
- $\text{Base}$ represents route baseline volume (750 to 4,800 rides/day).
- $M_{\text{day}}$ is the day-type factor (Weekday: 1.0; Saturday: 0.68; Sunday/Holiday: 0.48; with commuter routes dropping up to 0.28).
- $M_{\text{month}}$ reflects seasonal factors (winter dips, summer tourism, autumn university surge).
- $M_{\text{year}}$ represents system expansion (+3.8% in 2025).
- $\epsilon \sim \mathcal{N}(0, \sigma^2)$ is controlled Gaussian noise parameterized by route volatility.
- $M_{\text{event}}$ represents occasional high-demand civic shocks (e.g., marathons) or severe weather dips.

---

### 5. System Architecture & Methodology

The software architecture consists of four sequential execution stages:

1. **Synthetic Data Generator (`scripts/generate_data.py`):** Runs independently using Python standard libraries (`random`, `datetime`, `csv`) with seed 42 to ensure exact reproducibility.
2. **PySpark Session Manager (`src/spark_session.py`):** Centralizes SparkSession construction with local Windows optimizations (`local[*]`, driver host/bind address `127.0.0.1`, `spark.sql.shuffle.partitions = 4`, `spark.sql.ansi.enabled = false`).
3. **ETL Pipeline Orchestrator (`src/pipeline.py` & `src/data_loader.py`):**
   - Ingests raw CSV with explicit `StructType`.
   - Runs data validation rules and detects duplicates via window counting.
   - Quarantines invalid records into `quarantined_records.csv`.
4. **Analytics & SQL Transformation Engine (`src/analytics.py`):**
   - Executes PySpark DataFrame aggregations and registers temporary view `ridership`.
   - Executes ANSI Spark SQL queries for ranking, rolling time-series calculations, and day-type shares.
   - Exports summaries to `data/processed/`.
5. **Dashboard Layer (`app.py`):**
   - Streamlit interactive interface utilizing `@st.cache_data`.
   - Renders Plotly time-series charts, bar rankings, pie distributions, and route-month heatmaps.

---

### 6. Results and Analytical Findings

The pipeline was executed against the generated 14,620 observations. The derived empirical results are summarized below:

#### 6.1 System-Wide Performance Overview
- **Total System Passenger Volume:** 30,796,969 rides
- **Average Daily System Volume:** 42,129.92 rides/day
- **Monitored Transit Corridors:** 20 routes
- **Observation Days:** 731 calendar dates
- **Maximum Observed Single Route-Day Ridership:** 8,045 rides (Route R103 University Connector on peak university event day)
- **Minimum Observed Single Route-Day Ridership:** 201 rides (Route R120 Grand Boulevard on winter holiday)

#### 6.2 Route Demand Distribution (Top & Bottom Corridors)
| Rank | Route ID | Route Name | Total Rides | Daily Mean | Min Daily | Max Daily |
| :---: | :---: | :--- | :---: | :---: | :---: | :---: |
| 1 | R101 | 101 Metro Loop Trunk | 3,208,212 | 4,388.8 | 1,834 | 7,159 |
| 2 | R102 | 102 Harbor Express | 2,912,965 | 3,984.9 | 1,351 | 7,445 |
| 3 | R103 | 103 University Connector | 2,431,413 | 3,326.2 | 718 | 8,045 |
| 4 | R104 | 104 Crosstown Parkway | 2,409,691 | 3,296.4 | 1,297 | 5,789 |
| 5 | R105 | 105 Airport Shuttle Direct | 2,366,027 | 3,236.7 | 1,032 | 5,715 |
| ... | ... | ... | ... | ... | ... | ... |
| 18 | R118 | 118 Valley Industrial Link | 618,367 | 845.9 | 240 | 1,775 |
| 19 | R119 | 119 Riverfront Heritage Loop | 569,767 | 779.4 | 288 | 1,514 |
| 20 | R120 | 120 Grand Boulevard Crosstown | 501,475 | 686.0 | 201 | 1,328 |

#### 6.3 Operational Day-Type Breakdown
| Day Type | Distinct Service Days | Total Rides | Daily System Mean | Ridership Share (%) |
| :--- | :---: | :---: | :---: | :---: |
| **Weekday** | 507 | 24,841,137 | 48,996.3 | **80.66%** |
| **Saturday** | 104 | 3,309,499 | 31,822.1 | **10.75%** |
| **Sunday/Holiday** | 120 | 2,646,333 | 22,052.8 | **8.59%** |

**Interpretation:** Weekdays represent over 80% of total transit trips. Average Saturday system volume drops by ~35% compared to weekdays, and Sunday/Holiday volume drops by ~55%. Transit operators can use these metrics to reduce weekend bus dispatch frequencies while maintaining 100% capacity on primary trunk lines during weekday peak hours.

---

### 7. Testing & Verification

Automated testing was conducted using Pytest across 14 distinct test fixtures:
- `test_data_generation_reproducibility` (PASSED)
- `test_different_seeds_produce_different_data` (PASSED)
- `test_expected_record_count_and_columns` (PASSED)
- `test_unique_route_date_combinations` (PASSED)
- `test_holiday_and_day_type_classification` (PASSED)
- `test_missing_source_file_raises_error` (PASSED)
- `test_data_validation_quarantines_corrupted_rows` (PASSED)
- `test_compute_overall_metrics` (PASSED)
- `test_compute_route_summary` (PASSED)
- `test_compute_day_type_summary` (PASSED)
- `test_compute_monthly_summary` (PASSED)
- `test_top_and_bottom_routes` (PASSED)
- `test_pipeline_missing_file_raises_error` (PASSED)
- `test_pipeline_processed_outputs_exist_and_valid` (PASSED)

**Overall Test Suite Result:** `14 passed in 148.47s (100% success rate)`.

---

### 8. Limitations
1. **Local Mode Execution:** While the code utilizes standard PySpark DataFrame and Spark SQL APIs that are 100% cluster-compatible, testing was performed on a single Windows machine running `local[*]`.
2. **Synthetic Operational Scope:** The dataset is generated through deterministic simulation. While it models seasonal, holiday, and route volatility patterns, it does not represent real agency sensor telemetry.
3. **Daily Grain:** Data is aggregated at the route-day grain rather than individual GPS stop tap-in/tap-out event streams.

---

### 9. Future Scope
- **Streaming Telemetry:** Integrate Spark Structured Streaming with Apache Kafka to ingest live bus GPS feeds in real time.
- **Machine Learning Integration:** Train PySpark MLlib regression models (e.g., GBTRegressor, RandomForest) to forecast route-level demand 7 days in advance.
- **Geospatial Mapping:** Integrate GeoPandas and Kepler.gl to visualize route shapefiles, stop density, and transit congestion corridors on interactive 3D maps.

---

### 10. Conclusion
The **TransitPulse** project successfully demonstrates the full lifecycle of a Big Data Analytics engineering project. By pairing Apache PySpark's resilient distributed processing engine with Spark SQL, columnar Parquet storage, and an intuitive Streamlit interface, the application delivers actionable operational insights into public transit demand. The project meets all academic criteria, maintains strict reproducibility, includes automated quality verification, and provides a polished demonstration platform for college coursework evaluation.
