# 12 — Spark Performance Optimization

## Goal

Demonstrate ALL Spark performance optimization techniques from NEBo requirements by comparing a deliberately **unoptimized baseline** vs **fully optimized** version of the same Food Orders processing job.

---

## Quick Start (4 commands)

```bash
# Navigate to task directory
cd src/batch_processing/12_performance_optimization

# 1. Download + scale dataset (CSV for baseline, Parquet for optimized)
uv run python download_data.py

# 2. Start Spark cluster (master + worker)
docker compose up -d

# 3. Run BASELINE (slow, anti-patterns)
./run_baseline.sh

# 4. Run OPTIMIZED (fast, all techniques applied)
./run_optimized.sh
```

**Results** (dataset scaled to ~1,000,000 rows / ~63 MB so the optimizations actually matter):
- Baseline: ~9.5s
- Optimized: ~5.4s
- ~1.75x faster (~43% less wall-clock time), identical results

**Note:** The raw Kaggle file is only ~1,900 rows (120 KB) — at that size the whole runtime is fixed Spark startup overhead, so no optimization shows any effect. `download_data.py` inflates the data to a realistic volume; on multi-GB datasets the gap widens further as CSV parsing, shuffle and serialization start to dominate.

---

## File Structure

```
12_performance_optimization/
├── docker-compose.yml          ← Spark Standalone cluster (2 containers)
├── download_data.py            ← Fetch dataset from Kaggle
├── baseline_solution.py        ← UNOPTIMIZED (anti-patterns)
├── optimized_solution.py       ← OPTIMIZED (all NEBo techniques)
├── run_baseline.sh             ← Submit baseline to cluster
├── run_optimized.sh            ← Submit optimized to cluster
├── data/
│   ├── input/                  ← food_orders_large.csv + .parquet (+ raw)
│   └── output/                 ← baseline/ + optimized/ outputs
├── logs/                       ← baseline.log + optimized.log
└── README.md
```

---

## Optimization Techniques Applied

| NEBo Requirement | Baseline | Optimized | Impact |
|------------------|---------------|----------------|--------|
| **Serialization** | Java (default, slow) | Kryo (faster, smaller objects) | 10-30% faster shuffle |
| **API** | RDD + Python lambdas | DataFrame API (Catalyst optimizer) | 2-5x faster aggregations |
| **Caching** | Re-reads CSV 3 times | Read once, cache in memory | Eliminates redundant I/O |
| **Broadcast join** | Full shuffle on both sides | Broadcast small lookup table | Avoids shuffle on big side |
| **ByKey operations** | `groupByKey()` ships all values | `agg(F.avg())` combines map-side | Much less network traffic |
| **File format** | CSV (text, slow to parse) | Parquet (columnar, compressed) | 3-10x faster reads |
| **Partitions** | Default 200 shuffle partitions | Tuned to 4 (fits 2-core cluster) | Less task overhead |
| **AQE** | Disabled | Enabled (handles skew, coalesces) | Runtime optimization |

---

## Pipeline walkthrough — where each mechanism applies

Both jobs run the **same logical pipeline** on the same data and produce
**identical results** (612,374 rated orders, 1,200 customers). The only thing
that changes is the *mechanism* used at each stage.

**Session configuration (set once, applies to the whole job):**

| Setting | Baseline | Optimized | Why it helps |
|---|---|---|---|
| Serializer | Java (default) | Kryo | Smaller serialized objects, so less data travels across the network during shuffles |
| Shuffle partitions | 200 (default) | 4 | One task per partition; 200 tasks on a 2-core cluster is pure scheduling overhead |
| AQE | Off | On + coalesce | Spark re-plans at runtime: merges tiny partitions and handles data skew |
| Broadcast threshold | 10 MB (default) | 50 MB | Lets Spark auto-broadcast slightly larger lookup tables |

**Per-stage mechanisms:**

| Stage | Baseline (slow) | Optimized (fast) | Mechanism |
|---|---|---|---|
| 1. Load data | `read.csv` x3 with `inferSchema`, no cache | read **pre-built Parquet** once, then `.cache()` | Columnar + compressed file, parsed a single time, reused from memory |
| 2. Filter rated orders | `df.rdd.filter(lambda r: ...)` | `df.filter(F.col("rating") != ...)` | Catalyst pushes the filter down; no per-row Python lambda round-trip |
| 3. Avg cost per customer | `rdd.map(...).groupByKey().mapValues(mean)` | `groupBy("customer_id").agg(F.avg(...))` | Map-side combine ships only `(sum, count)` per key instead of every value |
| 4. Join cuisine lookup | `df.join(lookup)` — full shuffle on both sides | `df.join(F.broadcast(lookup))` | The 8-row table is copied to every executor; the big side never shuffles |
| 5. Write output | `write.csv` (text, uncompressed) | `write.parquet` (columnar, compressed) | Smaller files, faster to write and to read back |

---

## Baseline Anti-Patterns (What NOT to Do)

```python
# BAD: 1. Java serialization (default, slow)
.appName("Baseline")  # No Kryo config

# BAD: 2. Read CSV multiple times (no caching)
df1 = spark.read.csv(path)
df2 = spark.read.csv(path)  # Re-parses the same file!
df3 = spark.read.csv(path)

# BAD: 3. RDD filter with Python lambda (slow)
rated = df.rdd.filter(lambda r: r["rating"] != "Not given")

# BAD: 4. groupByKey() ships ALL values per key to one executor
avg_cost = (df.rdd
    .map(lambda r: (r["customer_id"], float(r["cost"])))
    .groupByKey()  # BAD: Sends entire lists across network
    .mapValues(lambda vs: sum(vs) / len(list(vs)))
)

# BAD: 5. Join without broadcast (full shuffle on both sides)
joined = df.join(lookup, on="cuisine_type")

# BAD: 6. Write CSV (uncompressed, text format)
df.write.csv(output_path)

# BAD: 7. Default 200 shuffle partitions (way too many for 2 cores)
```

---

## Optimized Techniques (Best Practices)

```python
# GOOD: 1. Kryo serialization
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# GOOD: 2. Read pre-built Parquet once + cache
#         (Parquet is created at prep time in download_data.py, not in the job)
df = spark.read.parquet(parquet_path).cache()
df.count()  # Materialize cache once

# GOOD: 3. DataFrame filter (Catalyst can optimize)
rated = df.filter(F.col("rating") != F.lit("Not given"))

# GOOD: 4. DataFrame agg (map-side combine, no full lists shipped)
avg_cost = df.groupBy("customer_id").agg(F.avg("cost_of_the_order").alias("avg_cost"))

# GOOD: 5. Broadcast join (small table sent to all executors)
joined = df.join(F.broadcast(lookup), on="cuisine_type")

# GOOD: 6. Write Parquet (columnar, compressed)
df.write.parquet(output_path)

# GOOD: 7. Tune shuffle partitions to cluster size
.config("spark.sql.shuffle.partitions", "4")  # 2 cores → 4 partitions

# GOOD: 8. AQE handles skew + coalesces small partitions at runtime
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

---

## How to Compare Performance

1. **Spark UI** — open http://localhost:4040 while job runs:
   - **Jobs tab** → see # of jobs, stages, tasks
   - **Stages tab** → shuffle read/write bytes, task duration
   - **SQL tab** → physical plan for DataFrame operations
   - **Storage tab** → cached DataFrames

2. **Logs** — check `logs/*.log`:
   - `baseline.log` → timing info
   - `optimized.log` → timing info

3. **Output size**:
   ```bash
   du -sh data/output/baseline/
   du -sh data/output/optimized/
   ```
   Parquet outputs are much smaller than CSV.

---

## Why Optimized is Faster (Explained)

### 1. Kryo Serialization
**What it does:** Converts Java objects to bytes when shuffling data between executors.  
**Why it matters:** Java serializer is verbose (large objects). Kryo is 5-10x more compact → less network transfer.

### 2. Parquet vs CSV
**What it does:** Columnar storage format (stores columns together, not rows).  
**Why it matters:**
- **Compressed** — 3-10x smaller than CSV
- **Columnar projection** — only reads columns you need (CSV reads entire row)
- **Splittable** — parallelizes reads across executors
- **Schema embedded** — no `inferSchema` parsing overhead

### 3. DataFrame API vs RDD
**What it does:** Spark's high-level API with Catalyst optimizer.  
**Why it matters:**
- **Catalyst** rewrites your query plan (predicate pushdown, filter before join, etc.)
- **Tungsten** generates bytecode for aggregations (10x faster than Python lambdas)
- **Whole-stage code generation** — fuses multiple operators into one tight loop

### 4. Broadcast Join
**What it does:** Sends small table to ALL executors instead of shuffling big table.  
**Why it matters:**
- Baseline: shuffles all ~1,000,000 rows from the big side across the network
- Optimized: sends the 8-row lookup once → no shuffle on the big side

### 5. `agg(F.avg())` vs `groupByKey()`
**What it does:** Map-side combine before shuffle.  
**Why it matters:**
- `groupByKey()` ships **entire lists** per key across network
- `agg(F.avg())` computes partial sums/counts on each executor → ships only (sum, count) → final avg computed after shuffle
- Example: 100 orders per customer → baseline ships 100 values, optimized ships 2 numbers

### 6. Caching
**What it does:** Keeps DataFrame in memory after first read.  
**Why it matters:** Baseline re-parses CSV 3 times. Optimized parses once, reuses in-memory copy.

### 7. Tuned Shuffle Partitions
**What it does:** Controls parallelism after shuffle operations.  
**Why it matters:**
- Default 200 partitions = 200 tasks → massive overhead for tiny dataset on 2 cores
- Tuned to 4 partitions = 4 tasks → minimal overhead, fits hardware

### 8. AQE (Adaptive Query Execution)
**What it does:** Spark 3.x feature that optimizes at runtime based on actual data stats.  
**Why it matters:**
- **Coalesces small partitions** — if shuffle produces 50 tiny partitions, AQE merges them into 4 bigger ones
- **Handles data skew** — if one key has 90% of data, AQE splits it across multiple tasks

---

## Requirements Coverage Checklist

| NEBo Requirement | Status | How Demonstrated |
|------------------|--------|------------------|
| Understand performance factors | Done | README explains volume, complexity, resources, latency |
| Configure for optimal performance | Done | Tuned `spark.sql.shuffle.partitions=4` for 2-core cluster |
| Optimize transformations/actions | Done | DataFrame API, broadcast join, agg vs groupByKey |
| Define storage types/partitioning | Done | Parquet (columnar, compressed), tuned partitions |
| Distinguish default vs non-default params | Done | Kryo vs Java, 4 vs 200 partitions, AQE enabled |
| Scale for larger volumes | Done | Parquet + cache enable horizontal scaling |
| Monitor performance | Done | Spark UI (4040) + timing captured in logs/*.log |
| Optimize cost efficiency | Done | Parquet reduces storage, tuned partitions reduce waste |
| Change serialization to Kryo | Done | `spark.serializer=KryoSerializer` |
| Use DataFrame API effectively | Done | `filter(F.col(...))`, `agg(F.avg(...))` vs RDD lambdas |
| Broadcast small lookup table | Done | `F.broadcast(lookup)` in join |
| Cache frequently accessed DFs | Done | `.cache()` + `.count()` to materialize |
| Optimize ByKey operations | Done | `agg(F.avg())` instead of `groupByKey().mapValues()` |
| Optimize file format | Done | Parquet vs CSV |
| Adjust partitions/parallelism | Done | `spark.sql.shuffle.partitions=4` |

**All 15 NEBo requirements: COVERED**

---

## Stop the cluster

```bash
# From src/batch_processing/12_performance_optimization
docker compose down
```
