# TransitPulse | Public Transport Ridership Pattern Analysis Using Apache PySpark

[![Python Version](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.0.4-orange.svg)](https://spark.apache.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.66.0-red.svg)](https://streamlit.io/)
[![Plotly](https://img.shields.io/badge/Plotly-7.1.0-blueviolet.svg)](https://plotly.com/)
[![Tests](https://img.shields.io/badge/Pytest-14%20Passed-brightgreen.svg)](https://pytest.org/)

An academic Big Data Analytics college mini-project demonstrating end-to-end distributed data processing, data quality validation, window analytics, Spark SQL queries, and interactive dashboard visualization using **Apache PySpark 4.0.4**, **Spark SQL**, and **Streamlit**.

> [!IMPORTANT]
> **ACADEMIC DEMONSTRATION DISCLAIMER:**
> This project utilizes a deterministic, mathematically simulated synthetic dataset representing fictional public bus corridors over two calendar years (2024–2025). The records are artificial and **do not** represent actual operational records of the Chicago Transit Authority (CTA) or any real-world transit agency.

---

## 1. Problem Statement
Urban transit authorities face critical scheduling and resource allocation challenges:
- High-capacity trunk corridors frequently suffer from severe passenger overcrowding during peak weekday commuting windows.
- Suburban feeder routes operate with under-utilized fleets on weekends and public holidays, inflating fuel and labor expenses.
- Traditional single-threaded data tools (such as standard pandas scripts or legacy spreadsheet workflows) struggle to process multi-year automated passenger counter (APC) datasets without memory exhaustion.

## 2. Project Objectives
- **Simulate Realistic Urban Demand:** Generate two full calendar years (2024–2025; 14,620 observations) across 20 fictional routes modeling seasonality, route tiers, day-type factors, and public holidays.
- **Distributed Ingestion & Quarantine:** Ingest raw CSV data via PySpark DataFrames with an explicit `StructType` schema, validating fields and quarantining corrupted/duplicate rows.
- **Big Data Analytics & Spark SQL:** Compute dense route rankings, 7-day rolling demand trends, month-over-month (MoM) growth using `LAG()`, and day-type shares.
- **Dual Persistence:** Save human-readable analytical summaries (CSV) and columnar storage (Apache Parquet).
- **Interactive Web Dashboard:** Build a responsive Streamlit dashboard with Plotly charts, sidebar filters, KPI cards, and CSV export capabilities.
- **Automated Testing:** Verify pipeline correctness and mathematical calculations using a comprehensive Pytest test suite.

## 3. Why Big Data Analytics is Relevant
In urban transit networks, millions of tap-in/tap-out smartcard events and GPS automated vehicle location (AVL) data points are recorded daily. 
1. **In-Memory Distributed Computation:** Spark's resilient distributed dataset abstractions and Catalyst DAG query optimizer allow parallel aggregation across CPU cores and cluster nodes.
2. **Elimination of Driver Bottlenecks:** Heavy transformations and window calculations run distributed across partitions; only compact aggregated metrics are collected into driver memory.
3. **Columnar Compression:** Storing clean processed data in Apache Parquet enables fast query scans, column projection, and Snappy compression.

---

## 4. Technologies Used
- **Programming Language:** Python 3.11.2 (via local virtual environment)
- **Distributed Computing Engine:** Apache PySpark 4.0.4
- **JVM Runtime:** Java JDK 21 LTS
- **Query Engine:** Spark SQL (Window functions: `DENSE_RANK`, `LAG`, `OVER()`)
- **Storage Formats:** Apache Parquet, CSV, JSON
- **User Interface:** Streamlit 1.66.0
- **Interactive Visualizations:** Plotly 7.1.0
- **Quality Assurance & Testing:** Pytest 9.1.1

---

## 5. Synthetic Dataset Overview
The synthetic dataset is generated deterministically (`seed=42`) using standard libraries:
- **Date Range:** 2024-01-01 to 2025-12-31 (731 calendar days, accurately accounting for the 2024 leap year).
- **Routes:** 20 fictional bus routes (Trunk, Express, University, Commuter, Shuttle, Local, Circulator, Feeder).
- **Total Records:** $731 \times 20 = 14,620$ unique route-date records.
- **Columns:**
  - `route_id`: Unique identifier (`R101` – `R120`)
  - `route_name`: Fictional corridor name (e.g., `101 Metro Loop Trunk`)
  - `service_date`: ISO-8601 date (`YYYY-MM-DD`)
  - `day_type`: Operational schedule category (`Weekday`, `Saturday`, `Sunday/Holiday`)
  - `rides`: Non-negative integer passenger count

---

## 6. Architecture & Data Flow
```
data/raw/ridership.csv 
       │
       ▼
[src/data_loader.py] ──(Validation & Quarantine)──► data/processed/quarantined_records.csv
       │
       ▼
[clean_df (Spark Memory Cache)]
       │
       ├─► [src/analytics.py] ──► Spark SQL Views, Aggregations, Window Functions
       │
       ▼
[src/pipeline.py] 
       │
       ├─► data/processed/route_summary.csv
       ├─► data/processed/monthly_summary.csv
       ├─► data/processed/day_type_summary.csv
       ├─► data/processed/daily_summary.csv
       ├─► data/processed/top_routes.csv / bottom_routes.csv
       ├─► data/processed/route_day_type_summary.csv
       ├─► data/processed/route_monthly_heatmap.csv
       ├─► data/processed/overall_metrics.json
       └─► data/processed/ridership.parquet
       │
       ▼
[app.py (Streamlit Dashboard)] ──► Interactive Web Browser UI
```

---

## 7. Folder Structure
```
public-transport-ridership/
│
├── app.py                      # Interactive Streamlit analytics dashboard
├── requirements.txt            # Tested dependencies matching environment
├── README.md                   # Comprehensive project documentation
├── .gitignore                  # Git ignore rules for venv, Spark, and caches
├── pytest.ini                  # Pytest configuration settings
├── spark_check.py              # Environment and PySpark verification script
│
├── scripts/
│   ├── __init__.py
│   └── generate_data.py        # Deterministic synthetic dataset generator
│
├── src/
│   ├── __init__.py             # Package initializer
│   ├── spark_session.py        # Centralized SparkSession manager
│   ├── data_loader.py          # Schema definition, ingestion & quarantine
│   ├── analytics.py            # DataFrame transformations & Spark SQL queries
│   └── pipeline.py             # Complete ETL orchestrator (CLI entrypoint)
│
├── tests/
│   ├── __init__.py
│   ├── test_data_generation.py # Tests for generation and holiday logic
│   ├── test_data_validation.py # Tests for schema validation and quarantine
│   ├── test_analytics.py       # Tests for KPIs, route rankings, and windows
│   └── test_pipeline.py        # Integration tests for processed files
│
├── data/
│   ├── raw/                    # Raw generated dataset (ridership.csv)
│   ├── sample/                 # Small preview sample (ridership_sample.csv)
│   └── processed/              # Aggregated CSVs, Parquet, and JSON metrics
│
└── docs/
    ├── architecture.md         # Mermaid diagrams and technical design
    ├── report.md               # Full academic mini-project report
    └── viva_questions.md       # 25 Viva Voce questions and technical answers
```

---

## 8. Prerequisites
- **Operating System:** Windows 10 or 11 (64-bit)
- **Python:** Python 3.11 (or 3.12)
- **Java Runtime:** Java JDK 17 or JDK 21 LTS installed and on `PATH`
  - Verify with: `java -version`

---

## 9. Setup & Installation (Windows PowerShell)

Open **Windows PowerShell** in the project directory:

### Step 1: Create and Activate Virtual Environment
```powershell
# Create project-local virtual environment
python -m venv .venv

# Activate the virtual environment
.\.venv\Scripts\Activate.ps1
```

> *Tip: If PowerShell blocks script activation, run: `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`*

### Step 2: Install Project Dependencies
```powershell
pip install -r requirements.txt
```

---

## 10. Execution Commands

### Step 3: Generate the Synthetic Dataset
```powershell
python scripts/generate_data.py
```
*Outputs: `data/raw/ridership.csv` (14,620 rows) and `data/sample/ridership_sample.csv` (50 rows).*

### Step 4: Execute the PySpark ETL Pipeline
```powershell
python -m src.pipeline
```
*Executes Spark ingestion, schema validation, analytical transformations, SQL queries, and saves processed CSVs and Parquet files.*

### Step 5: Launch the Streamlit Dashboard
```powershell
streamlit run app.py
```
*Opens the web dashboard in your default browser at `http://localhost:8501`.*

### Step 6: Run Automated Tests
```powershell
pytest -v
```
*Executes all 14 unit and integration tests across data generation, validation, analytics, and pipeline outputs.*

---

## 11. Actual Pipeline Results & Findings

From executing the pipeline on the synthetic dataset (Seed: 42):
- **Total Passenger Volume:** 30,796,969 rides
- **Average Daily System Rides:** 42,129.92 rides/day
- **Monitored Routes:** 20 bus corridors
- **Service Dates:** 731 calendar days
- **Highest Volume Route:** `R101` (*101 Metro Loop Trunk*) carrying **3,208,212** total rides (**4,388.8** rides/day).
- **Day-Type Distribution:**
  - **Weekday:** 24,841,137 rides (**80.66%** system share, avg 48,996 rides/day)
  - **Saturday:** 3,309,499 rides (**10.75%** system share, avg 31,822 rides/day)
  - **Sunday/Holiday:** 2,646,333 rides (**8.59%** system share, avg 22,053 rides/day)

---

## 12. Troubleshooting & Environment Tips

| Issue | Cause | Solution |
| :--- | :--- | :--- |
| `java: command not found` | Java JDK is not installed or not added to system PATH | Install Java JDK 17 or 21 LTS (e.g., Eclipse Temurin or Oracle JDK) and add `bin` to Windows PATH. |
| `winutils.exe: FileNotFoundException` | Standard Hadoop warning on Windows | This is a harmless warning on local Windows PySpark runs. The pipeline uses PyArrow for Parquet serialization to avoid requiring Hadoop native binaries. |
| Spark IPv6 / Hostname Stall | Windows hostname lookup delays | Centralized in `src/spark_session.py`: `spark.driver.host` and `bindAddress` are locked to `127.0.0.1`. |
| Port 8501 Already in Use | Another Streamlit instance is running | Run on a different port: `streamlit run app.py --server.port 8502`. |

---

## 13. Instructions for Uploading to GitHub

To publish this project to your GitHub repository:

```powershell
# 1. Initialize git (if not already initialized)
git init

# 2. Add files (respecting .gitignore which excludes .venv, caches, etc.)
git add .

# 3. Commit changes
git commit -m "feat: complete Public Transport Ridership Pattern Analysis with PySpark and Streamlit"

# 4. Create a new repository on GitHub (e.g., https://github.com/your-username/public-transport-ridership)
# 5. Link remote repository and push:
git remote add origin https://github.com/<your-username>/public-transport-ridership.git
git branch -M main
git push -u origin main
```

---

## 14. Academic Integrity & Project Documentation
- Detailed Technical Architecture: [`docs/architecture.md`](docs/architecture.md)
- Complete College Mini-Project Report: [`docs/report.md`](docs/report.md)
- Viva Examination Questions (25 Q&A): [`docs/viva_questions.md`](docs/viva_questions.md)
