# 11 — Spark Cluster (Docker) + Spark UI Debug

## Goal

Run a Spark job on a Standalone cluster (Dockerized) and debug it via the Spark UI (port 4040).

---

## Prerequisites

We use **Colima** (free, open-source Docker runtime) instead of Docker Desktop (license required).

```bash
# Install (if not already installed)
brew install docker docker-compose colima

# Start Colima VM (only if not running)
colima start
```

> **Note:** If `docker` / `colima` are not in PATH, run: `eval "$(/opt/homebrew/bin/brew shellenv)"`

---

## Quick Start

```bash
# 0. Go to the task directory
cd src/batch_processing/

# 1. Start the cluster (master + worker)
docker compose up -d

# 2. Submit the job
./spark_submit.sh

# 3. Open Spark UI in browser (you have 60 seconds)
open http://localhost:4040
```

After 60 seconds the driver exits and Spark UI disappears. You can interrupt earlier with `Ctrl+C`.

**Master UI** (list of workers and applications): http://localhost:8080

---

## File Structure

```
11_spark_cluster/
├── docker-compose.yml    ← defines the cluster: 1 master + 1 worker
├── spark_ui_debug.py     ← Spark job (5 jobs targeting different UI tabs)
├── spark_submit.sh       ← script to submit the job (docker exec)
├── data/output/          ← computation results (JSON, CSV, Parquet)
│   ├── dept_stats/       ← per-department statistics (JSON)
│   ├── sql_aggregation/  ← SQL query result (CSV)
│   └── skew_analysis/    ← skew analysis (Parquet)
└── README.md
```

---

## Code Walkthrough — docker-compose.yml

```yaml
services:
  spark-master:
    image: apache/spark-py:latest       # official Spark image with Python support
    container_name: pex-spark-master    # container name (used by docker exec)
    hostname: spark-master              # hostname visible in Docker network
    user: root                          # root — needed for write permissions
    command: /opt/spark/sbin/start-master.sh  # launches the Master process
    ports:
      - "7077:7077"                     # Spark protocol port (master↔worker communication)
      - "8080:8080"                     # Master Web UI (worker list)
      - "4040:4040"                     # Spark UI (job debugging)
    volumes:
      - ./spark_ui_debug.py:/opt/spark_job/spark_ui_debug.py:ro
        # ↑ mounts our Python file into the container (read-only)
      - ./data/output:/opt/spark_job/output
        # ↑ output directory — visible on the host
      - spark-events:/tmp/spark-events
        # ↑ Spark event logs (for History Server)
    environment:
      - SPARK_NO_DAEMONIZE=true         # don't daemonize — Docker needs a foreground process
      - SPARK_MASTER_OPTS=-Dspark.ui.port=8080

  spark-worker:
    image: apache/spark-py:latest
    container_name: pex-spark-worker
    hostname: spark-worker
    user: root
    command: /opt/spark/sbin/start-worker.sh spark://spark-master:7077
      # ↑ registers with the master at port 7077
    ports:
      - "8081:8081"                     # Worker Web UI
    depends_on:
      - spark-master                    # worker starts AFTER master
    volumes:
      - ./data/output:/opt/spark_job/output
        # ↑ same directory — because executor (worker) writes the data
      - spark-events:/tmp/spark-events
    environment:
      - SPARK_NO_DAEMONIZE=true
      - SPARK_WORKER_CORES=2            # worker gets 2 CPU cores
      - SPARK_WORKER_MEMORY=2g          # worker gets 2 GB RAM
```

**How it works:**
1. `docker compose up -d` spins up 2 containers in one Docker network.
2. Master listens on port 7077 — waits for workers to register.
3. Worker starts and says: "hey master, I'm here, I have 2 CPUs and 2GB RAM".
4. When we submit a job, master assigns tasks to the worker.

---

## Code Walkthrough — spark_submit.sh

```bash
docker exec pex-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.ui.port=4040 \
    /opt/spark_job/spark_ui_debug.py
```

- `docker exec pex-spark-master` → enter the master container and run a command inside it
- `/opt/spark/bin/spark-submit` → Spark's tool for submitting jobs
- `--master spark://spark-master:7077` → "connect to the master at this address"
- `--deploy-mode client` → driver (the managing program) runs inside the master container
  (this way Spark UI lives on the master → port 4040 is visible on the host)
- `--conf spark.ui.port=4040` → force port 4040 for Spark UI
- `/opt/spark_job/spark_ui_debug.py` → our script mounted from the host

---

## Code Walkthrough — spark_ui_debug.py (line by line)

### Spark Session

```python
spark = (
    SparkSession.builder
    .appName("SparkUIDebug")
    # ↑ name visible in Master UI and Spark UI

    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    # ↑ faster serializer (converts objects to bytes when transferring between nodes)

    .config("spark.sql.shuffle.partitions", "8")
    # ↑ how many partitions after shuffle (default is 200 — too many for a small dataset)

    .config("spark.eventLog.enabled", "true")
    # ↑ save events to file (needed for History Server)

    .config("spark.eventLog.dir", "file:///tmp/spark-events")
    # ↑ where to save events

    .config("spark.ui.port", "4040")
    # ↑ Spark UI on port 4040

    .getOrCreate()
    # ↑ create session (or reuse existing one)
)
```

### JOB 1 — Create data + count

```python
data = [
    (f"EMP_{i:04d}", dept, 40000 + (i * 137) % 60000)
    # ↑ generates tuples: ("EMP_0001", "Engineering", 54680)
    #   - f"EMP_{i:04d}" → employee ID with leading zeros (EMP_0001, EMP_0002...)
    #   - dept → department name
    #   - 40000 + (i * 137) % 60000 → salary 40k-100k (pseudo-random)
    for i in range(500)           # 500 employees
    for dept in ["Engineering", "Marketing", "Sales", "HR", "Finance"]
    # ↑ each employee in each department = 500 × 5 = 2500 rows
]
df = spark.createDataFrame(data, ["employee_id", "department", "salary"])
# ↑ create DataFrame with columns: employee_id, department, salary

print(f"    Rows: {df.count()}")
# ↑ count() is an ACTION — forces computation → creates a JOB in Spark UI
```

**Why this matters for UI:** `count()` is an action, so it creates a "Job" visible
in the Jobs tab. One action = one job.

### JOB 2 — SQL (SQL tab in UI)

```python
df.createOrReplaceTempView("employees")
# ↑ register DataFrame as a SQL table named "employees"

result = spark.sql("""
    SELECT department,
           COUNT(*)              AS cnt,         -- employee count
           ROUND(AVG(salary), 2) AS avg_salary,  -- average salary
           MAX(salary)           AS max_salary,  -- highest salary
           MIN(salary)           AS min_salary   -- lowest salary
    FROM employees
    GROUP BY department       -- group by department
    ORDER BY avg_salary DESC  -- sort descending by average
""")
result.show()
# ↑ show() is an action → new Job + SQL tab shows the execution plan
```

**Why this matters for UI:** Using `spark.sql()` populates the **SQL** tab in Spark UI —
you can see the logical and physical execution plan.

### JOB 3 — GroupBy + cache (Storage tab)

```python
dept_stats = (
    df.groupBy("department")        # group rows by department column
    .agg(
        F.count("*").alias("count"),            # count rows
        F.avg("salary").alias("avg_salary"),    # average salary
        F.stddev("salary").alias("std_salary"), # standard deviation
    )
)
dept_stats.cache()
# ↑ CACHE = keep result in RAM (don't recompute every time)
#   Will appear in the Storage tab in Spark UI

dept_stats.show()
# ↑ show() forces computation + loads data into cache

print(f"    Cached partitions: {dept_stats.rdd.getNumPartitions()}")
# ↑ how many partitions the DataFrame has (= how many "chunks" of data)
```

**Why this matters for UI:** After `cache()` + action, the **Storage** tab shows
the cached DataFrame with information about memory usage.

### JOB 4 — Data skew (uneven data distribution)

```python
skewed = df.withColumn(
    "skew_key",
    F.when(F.col("department") == "Engineering", F.lit("HOT"))
     .otherwise(F.col("department"))
)
# ↑ Add a skew_key column:
#   - if department = "Engineering" → insert "HOT"
#   - otherwise → keep original department name
#   Effect: key "HOT" gets disproportionate data — simulates real-world skew

skew_result = skewed.groupBy("skew_key").agg(F.sum("salary").alias("total"))
# ↑ sum of salaries per key — forces shuffle (data transfer between nodes)

skew_result.show()
```

**Why this matters for UI:** In the **Stages** tab you can see that some tasks
take longer (straggler tasks) — a classic "data skew" problem in Spark.

### JOB 5 — Write results

```python
output_path = "/opt/spark_job/output"

dept_stats.coalesce(1).write.mode("overwrite").json(f"{output_path}/dept_stats")
# ↑ coalesce(1) = merge all partitions into 1 file (instead of 8 files)
#   .write.mode("overwrite") = overwrite if exists
#   .json(...) = save as JSON

result.coalesce(1).write.mode("overwrite").csv(f"{output_path}/sql_aggregation", header=True)
# ↑ SQL result → CSV with header

skew_result.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/skew_analysis")
# ↑ skew result → Parquet (columnar format, compressed)
```

### Sleep — keep UI alive

```python
time.sleep(60)
# ↑ Driver (our program) does NOT exit immediately.
#   Spark UI only works while the driver is alive.
#   We give 60 seconds to browse the UI in the browser.

spark.stop()
# ↑ clean up resources and close connection to the cluster
```

---

## Spark UI Tabs — What You'll See

| Tab | Content | Which job generates it |
|-----|---------|----------------------|
| **Jobs** | List of all jobs (count, show, write) | Every action = 1 job |
| **Stages** | Individual stages within jobs (map, shuffle, reduce) | All jobs |
| **Storage** | Cached DataFrames (size in RAM) | Job 3 (cache) |
| **Environment** | Spark configuration (KryoSerializer, partitions) | Session |
| **Executors** | List of executors (driver + worker), RAM/CPU usage | Worker |
| **SQL** | Execution plan for SQL queries | Job 2 (spark.sql) |

---

## Requirements

- Docker + Docker Compose (via Colima or Docker Desktop)
- Free ports: 4040, 7077, 8080, 8081

## Stop the cluster

```bash
docker compose down
```
