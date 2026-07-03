# 12 — Concurrency and Multithreading

## Task

Develop a Spark application that uses concurrency and multithreading to process
large volumes of data in parallel. Implement explicit communication mechanisms
between threads (shared memory, message passing).

## Concurrency Patterns Used

### 1. ThreadPoolExecutor (concurrent.futures)

**Model**: Thread pool with a fixed number of workers (4).
Each analysis function is submitted as a task to the pool. Spark's internal
parallelism handles data-level distribution; Python threads handle
task-level parallelism (running multiple Spark actions concurrently).

**Benefit**: Simple API, automatic thread management, `as_completed()` for
processing results as they arrive.

**Trade-off**: Python GIL limits CPU-bound parallelism, but Spark actions
release the GIL (they block on JVM calls), so thread pool works well here.

### 2. threading.Lock (shared memory)

**Model**: `ResultsCollector` class uses a Lock to protect a shared dictionary
where threads store their results.

**Benefit**: Prevents race conditions when multiple threads write to the same
dictionary concurrently.

**Trade-off**: Lock contention could be a bottleneck with many threads, but
with 4 workers and fast dict writes, it is negligible.

### 3. queue.Queue (message passing)

**Model**: Producer-consumer pattern. Analysis threads (producers) put
`(name, DataFrame)` tuples into a Queue. A dedicated saver thread (consumer)
reads from the queue and writes results to CSV.

**Benefit**: Decouples analysis from I/O. Analyses don't block waiting for
CSV writes. Thread-safe by design (Queue handles synchronization internally).

**Trade-off**: Adds complexity of a consumer thread + stop signaling.

## Results

| Analysis | Description | Output |
|----------|-------------|--------|
| `by_currency` | Transaction volume by receiving currency | Aggregation |
| `by_payment_format` | Avg amount and count by payment format | Aggregation |
| `laundering_ratio` | Laundering percentage per bank | Filter + Aggregate |
| `top_senders` | Top 20 sender accounts by total paid | Sort + Limit |
| `high_value_txns` | Transactions above 1M | Filter + Sort |
| `hourly_volume` | Transaction volume by hour of day | Transform + Aggregate |

## Dataset

**IBM AML Transactions** (`HI-Large_Trans.csv`) — ~180M rows, 11 columns.
Source: [Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)

## Layout

```
12_concurrency/
├── download_data.py      # Kaggle download script
├── solution.py           # Concurrent PySpark application
├── README.md
├── data/
│   ├── input/            # HI-Large_Trans.csv
│   └── output/           # 6 analysis results
└── logs/
    └── concurrency.log
```

## Prerequisites

- uv (package manager)
- Java 17: `export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"`
- Kaggle API token at `~/.kaggle/kaggle.json`
- ~16 GB disk space for dataset

## Run

```bash
# Download data
uv run python src/programming_languages/12_concurrency/download_data.py

# Run application
uv run python src/programming_languages/12_concurrency/solution.py
```

## `solution.py` walkthrough

| Component | Logic |
|-----------|-------|
| `ResultsCollector` | Thread-safe dict with Lock for collecting results |
| `analyze_by_currency()` | groupBy currency, sum + count |
| `analyze_by_payment_format()` | groupBy format, avg + count |
| `analyze_laundering_ratio()` | groupBy bank, compute laundering % |
| `analyze_top_senders()` | groupBy account, sum, orderBy, limit 20 |
| `analyze_high_value_txns()` | filter > 1M, select, orderBy |
| `analyze_hourly_volume()` | extract hour, groupBy, sum + count |
| `save_worker()` | Consumer thread reading from Queue, saves CSV |
| `main()` | Loads data, starts saver thread, submits analyses to ThreadPoolExecutor |

## Acceptance Criteria

| Criterion | How Met |
|-----------|---------|
| Concurrent processing implemented | ThreadPoolExecutor with 4 workers runs 6 analyses in parallel |
| Communication mechanisms implemented | Lock (shared ResultsCollector) + Queue (producer-consumer saver) |
| Thread safety ensured | Lock protects shared dict; Queue is inherently thread-safe |
| Concurrency models documented | This README documents 3 patterns with benefits and trade-offs |
| Tested and validated | All 6 analyses complete successfully on 180M rows in ~48s |
| Design decisions documented | Each pattern choice explained with rationale |

## Implementation Notes

- Spark actions (groupBy, count, write) release the Python GIL, making
  threading effective despite Python's GIL limitation.
- The dataset (180M rows) is cached after load to avoid re-reading CSV
  for each concurrent analysis.
- `save_worker` uses `stop_event + queue.join()` to ensure all results are
  saved before the application exits.
- `spark.driver.memory=4g` prevents OOM on the large dataset.
