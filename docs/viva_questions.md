# Viva Voce Examination Questions & Answers

**Project Title:** Public Transport Ridership Pattern Analysis Using Apache PySpark  
**Subject:** Big Data Analytics / Distributed Computing  

---

### 1. What is Apache Spark, and why is it preferred over traditional MapReduce?
**Answer:** Apache Spark is an open-source, distributed general-purpose computing engine. Unlike Hadoop MapReduce, which writes intermediate calculation states to disk after every Map or Reduce step, Spark performs in-memory computations using directed acyclic graphs (DAGs), achieving up to 100x faster performance for iterative workloads and analytics.

---

### 2. What is PySpark, and how does Python communicate with the underlying JVM?
**Answer:** PySpark is the Python API for Apache Spark. Python communicates with the underlying Java Virtual Machine (JVM) through **Py4J**. The driver process runs in Python, issues commands across a local socket to a Java gateway, and executors serialize data between the JVM and Python worker processes when executing Python User Defined Functions (UDFs).

---

### 3. What is the difference between Spark Transformations and Actions?
**Answer:**
- **Transformations** (e.g., `filter()`, `select()`, `groupBy()`, `withColumn()`): Operations on a DataFrame that produce a new DataFrame without immediately computing the result. They are lazily evaluated.
- **Actions** (e.g., `count()`, `collect()`, `show()`, `write()`): Operations that trigger the execution of the computation plan built by transformations and return values to the driver or write data to storage.

---

### 4. What is Lazy Evaluation, and what are its performance advantages?
**Answer:** Lazy evaluation means Spark postpones physical execution until an action is invoked. This enables Spark's **Catalyst Optimizer** to analyze the complete lineage graph, combine operations, push down filter predicates, prune unnecessary columns, and select the most efficient physical execution plan before executing data reads.

---

### 5. What is a Spark DataFrame, and how does it differ from an RDD?
**Answer:** An RDD (Resilient Distributed Dataset) is an immutable, distributed collection of raw JVM objects with low-level functional API controls but no schema awareness. A Spark DataFrame is an RDD organized into named columns with an enforced schema, optimized by the Catalyst query optimizer and Tungsten execution engine for columnar memory allocation and code generation.

---

### 6. What is the role of Spark SQL, and how was it utilized in this project?
**Answer:** Spark SQL is a Spark module for structured data processing. It allows querying DataFrames using ANSI SQL syntax. In this project, we registered temporary views (`clean_df.createOrReplaceTempView("ridership")`) and ran SQL queries for ranking routes with `DENSE_RANK()`, calculating month-over-month growth with `LAG()`, and evaluating day-type percentage shares.

---

### 7. What is the difference between `RANK()` and `DENSE_RANK()` in window operations?
**Answer:** When values tie, `RANK()` skips subsequent ranks (e.g., 1, 2, 2, 4), whereas `DENSE_RANK()` does not skip rank values (e.g., 1, 2, 2, 3). In our route rankings, `DENSE_RANK()` ensures consecutive integer ranks without gaps regardless of identical ridership counts.

---

### 8. Explain how the `LAG()` analytical window function works.
**Answer:** `LAG(col, offset)` accesses a column value from a preceding row within the same window partition or ordered sequence without requiring a self-join. In our project, `LAG(total_rides, 1) OVER (ORDER BY year, month)` accesses the previous month's ridership to compute Month-over-Month (MoM) growth percentage.

---

### 9. Why is `.master("local[*]")` used, and what does it mean?
**Answer:** `.master("local[*]")` configures Spark to run in local simulation mode on a single machine, utilizing all available CPU logical cores as worker execution threads (e.g., 8 threads on an 8-core machine). It eliminates the overhead of provisioning external cluster infrastructure while demonstrating distributed pipeline architecture.

---

### 10. What are Driver and Executors in Apache Spark?
**Answer:**
- **Driver:** The master process containing `main()` and `SparkSession`. It maintains application state, converts user code into DAG tasks, schedules stages, and coordinates executors.
- **Executors:** Worker processes responsible for executing assigned tasks in parallel, storing cached data partitions in RAM/disk, and returning metrics to the driver.

---

### 11. What is the difference between CSV and Apache Parquet storage formats?
**Answer:**
- **CSV:** Row-oriented plain text format. It is human-readable but uncompressed, lacks built-in schema metadata, requires full file scanning, and is slow to parse.
- **Parquet:** Columnar binary storage format. It supports efficient dictionary encoding and Snappy compression, stores schema and min/max column statistics in file footers, and enables **predicate pushdown** and **column projection** (skipping unneeded columns during reads).

---

### 12. Why did we avoid collecting the entire raw dataset into driver memory with `.collect()`?
**Answer:** Calling `.collect()` on massive distributed datasets transfers all partitions across network sockets to the driver's JVM heap memory, causing catastrophic Out-Of-Memory (`OutOfMemoryError: Java heap space`) crashes. In Big Data best practices, all heavy filtering and aggregations occur across worker partitions, and only aggregated small summaries are collected.

---

### 13. How did the project handle data validation and quarantine corrupted records?
**Answer:** We defined an explicit `StructType` schema and validated mandatory fields, ISO date formatting, non-negative ride counts, and unique `(route_id, service_date)` pairs. Corrupted or duplicate rows are identified with a conditional rule, annotated with a `rejection_reason`, and segregated into `data/processed/quarantined_records.csv` rather than being silently dropped.

---

### 14. What are shuffle operations, and why should shuffle partitions be tuned?
**Answer:** A shuffle occurs when data must be redistributed across physical partitions (e.g., during `groupBy()`, `join()`, or `distinct()`). By default, Spark sets `spark.sql.shuffle.partitions` to 200, which creates excessive scheduling overhead for local or moderate data volumes. Tuning this setting to 4 in local mode speeds up stage completion significantly.

---

### 15. How does the 7-day rolling average window function work in PySpark?
**Answer:** We define a window frame ordered chronologically by `service_date` with `rowsBetween(-6, 0)`. For each calendar day, Spark averages the current row and the preceding 6 days of system ridership, smoothing out weekend dips to reveal the true baseline trend.

---

### 16. What statistical rule was implemented to detect ridership anomalies?
**Answer:** We computed the global mean ($\mu$) and standard deviation ($\sigma$) of daily system ridership. Days where ridership exceeded $\mu + 2\sigma$ were flagged as high-demand anomalies (e.g., city festivals or marathons), while days below $\mu - 2\sigma$ were flagged as low-demand dips (e.g., severe weather disruption).

---

### 17. Why is the dataset synthetic, and how was it generated deterministically?
**Answer:** The dataset was generated to simulate realistic public transit demand across 20 routes over two full years (2024–2025; 14,620 observations). A fixed pseudorandom seed (`seed=42`) guarantees exact mathematical reproducibility across runs. It models day-type factors, monthly seasonality, route baseline tiers, and federal holiday schedules.

---

### 18. Why was Streamlit selected for the user interface, and how is caching utilized?
**Answer:** Streamlit enables rapid Python-native dashboard development with reactive component re-rendering. We used `@st.cache_data` to load precomputed analytical datasets into memory once. When users adjust sidebar sliders or route filters, the dashboard queries cached pandas DataFrames instantly in sub-second time without relaunching the Spark JVM.

---

### 19. What Windows-specific configuration was required to make PySpark stable?
**Answer:** On Windows environments without Hadoop `winutils.exe`, network lookups can hang if IPv6 binds improperly. We configured:
- `spark.driver.host = "127.0.0.1"` and `spark.driver.bindAddress = "127.0.0.1"`
- `PYSPARK_PYTHON` set to the project virtual environment Python path
- `spark.sql.ansi.enabled = "false"` and regex date guards to prevent ANSI parsing crashes.

---

### 20. What is the difference between Narrow and Wide dependencies in Spark?
**Answer:**
- **Narrow Dependency:** Each partition of the parent RDD is used by at most one partition of the child RDD (e.g., `map()`, `filter()`). No data shuffle across nodes is required.
- **Wide Dependency:** Multiple child partitions depend on data from a single parent partition (e.g., `groupByKey()`, `reduceBy()`). This requires a data shuffle across the cluster network.

---

### 21. How did the pipeline account for the 2024 leap year?
**Answer:** The calendar generator dynamically accounted for 2024 having 366 days (including February 29) and 2025 having 365 days, yielding exactly 731 calendar days. Multiplied across 20 routes, this yields exactly $731 \times 20 = 14,620$ unique records.

---

### 22. What is Broadcast Join in Apache Spark, and when would you use it?
**Answer:** A broadcast join copies a small DataFrame to all executor nodes, avoiding an expensive shuffle of the large DataFrame across the network. It is typically triggered automatically when a joined table is below `spark.sql.autoBroadcastJoinThreshold` (default 10 MB).

---

### 23. What are the key limitations of this mini-project?
**Answer:**
1. Runs in Spark local mode rather than a multi-node YARN or Kubernetes cluster.
2. Uses synthetic simulated data rather than live automated vehicle location (AVL) / automatic passenger counter (APC) sensor streams.
3. Daily aggregation granularity rather than hourly stop-by-stop passenger tap-in/tap-out telemetry.

---

### 24. What are the operational insights revealed by the processed data?
**Answer:**
- Weekday service generates over 80% of total system ridership, confirming heavy commuting reliance.
- Saturday ridership operates at ~68% of weekday levels, while Sunday/Holiday operations drop to ~48%.
- High-capacity trunk routes (e.g., Route 101 Metro Loop Trunk) carry over 4,000 riders/day, while feeder routes carry ~800–1,200 riders/day.

---

### 25. How would you scale this architecture to process billions of real transit records?
**Answer:** Deploy Spark on an Elastic MapReduce (EMR) or Databricks cluster backed by Amazon S3 or Google Cloud Storage. Ingest streaming APC telemetry via Apache Kafka into Spark Structured Streaming, partition Parquet or Delta Lake tables by `year/month`, and use distributed Delta Lake caching for interactive BI queries.
