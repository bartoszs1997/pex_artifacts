# Fraudulent Transactions — Batch Processing (Spark RDD, cluster mode)

Spark batch application that scans a large payment-transactions dataset with the
**RDD API** and flags accounts whose spending pattern looks suspicious. It is run
on a **Spark Standalone cluster** and submitted with `spark-submit`, to show how
the job maps onto Spark components (driver + executor on a master/worker).

## Task

Analyse customer transactions to identify potentially fraudulent ones, using the
**Spark RDD** API (not DataFrames), and run the job in **cluster mode**:

- **Step 1** — filter transactions whose amount exceeds a threshold (**$10,000**).
- **Step 2** — compute the **average transaction amount per account** (`nameOrig`).
- **Step 3** — flag accounts whose average is **at least twice** the overall
  average transaction amount.

The flagged accounts and their average amounts are written to **CSV**.

## Results

| Metric | Value |
| --- | --- |
| Total rows in dataset | 6 362 620 |
| Step 1 — transactions with amount > $10 000 | **5 076 529** |
| Step 2 — distinct accounts after grouping | **5 070 636** |
| Overall average transaction amount (of the filtered txns) | **$224 237.21** |
| Step 3 — accounts with average ≥ 2× overall (≥ $448 474.42) | **419 382** |

Top flagged accounts (account → average amount):

| Account | Average amount |
| --- | --- |
| C1715283297 | 92 445 516.64 |
| C2127282686 | 73 823 490.36 |
| C2044643633 | 71 172 480.42 |
| C1425667947 | 69 886 731.30 |
| C1584456031 | 69 337 316.27 |

> All four headline numbers were cross-checked with an **independent pure-pandas
> pass** over the same file (reading only the `amount` and `nameOrig` columns) —
> identical: `5 076 529 / 5 070 636 / 224 237.21 / 419 382`.

## Dataset

Kaggle [`vardhansiramdasu/fraudulent-transactions-prediction`](https://www.kaggle.com/datasets/vardhansiramdasu/fraudulent-transactions-prediction)
— the **PaySim** synthetic mobile-money log, `Fraud.csv` (~471 MB, 6 362 620
rows). Columns:

```
step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud
```

This job only needs two of them: **`amount`** (column 2) and **`nameOrig`**
(column 3 — the originating account, used as the "account number").

> **Caveat — accounts are almost unique.** `nameOrig` has ~6.35 M distinct values
> over ~6.36 M rows (max 3 repeats for any single account). So "average per
> account" equals a single transaction amount for almost every account, and the
> two readings of "overall average" — mean of all filtered *transactions*
> ($224 237.21) vs. mean of the per-*account* averages ($224 231.14) — give
> essentially the same flag count (419 382 vs. 419 403). We use the
> transaction-level overall average, the most direct reading of the task.

## Layout

```
07_fraudulent_transactions_predictions/
├── download_data.py   # fetch Fraud.csv from Kaggle -> data/input/
├── solution.py        # Spark RDD pipeline: filter -> avg/account -> flag -> CSV
├── data/              # gitignored
│   ├── input/Fraud.csv
│   └── output/flagged_accounts/   # CSV: account,avg_amount
└── logs/              # gitignored
    └── fraud.log
```

## Prerequisites

- **Python** managed by `uv` (`uv sync` at the repo root).
- **Java 17** on `PATH` (required by PySpark 4.x):
  ```bash
  export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
  export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
  ```
- A **full Spark distribution** for the Standalone cluster scripts
  (`sbin/start-master.sh`, `sbin/start-worker.sh`) that the pip/uv `pyspark`
  wheel does not ship. The distribution version must match the pinned `pyspark`
  (4.1.2):
  ```bash
  cd ~
  curl -fLO https://archive.apache.org/dist/spark/spark-4.1.2/spark-4.1.2-bin-hadoop3.tgz
  tar xzf spark-4.1.2-bin-hadoop3.tgz
  export SPARK_HOME="$HOME/spark-4.1.2-bin-hadoop3"
  ```

## Run

### 0) Download the dataset

```bash
uv run python src/batch_processing/07_fraudulent_transactions_predictions/download_data.py
```

### Quick local run (single JVM, `local[*]`)

Good for verifying the logic; the master is taken from the environment and
defaults to `local[*]` (no cluster needed):

```bash
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/batch_processing/07_fraudulent_transactions_predictions/solution.py
```

### Cluster mode — Standalone (step by step)

Keep `SPARK_LOCAL_IP=127.0.0.1` so every component binds to loopback (avoids the
macOS LAN-IP networking issues on a single node).

**1) Start the master**

```bash
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export SPARK_HOME="$HOME/spark-4.1.2-bin-hadoop3"
export SPARK_LOCAL_IP=127.0.0.1

"$SPARK_HOME/sbin/start-master.sh" --host 127.0.0.1
```

Master at **`spark://127.0.0.1:7077`**, web UI at **http://localhost:8080**.

**2) Start a worker and register it**

```bash
export SPARK_LOCAL_IP=127.0.0.1
"$SPARK_HOME/sbin/start-worker.sh" spark://127.0.0.1:7077
```

The cluster is now up: one master + one worker (visible under "Workers" at
http://localhost:8080).

**3) Submit the application**

Point `PYSPARK_PYTHON` at this project's venv so the executors use the same
interpreter, then submit against the master URL:

```bash
export PYSPARK_PYTHON="$(pwd)/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$(pwd)/.venv/bin/python"

"$SPARK_HOME/bin/spark-submit" \
    --master spark://127.0.0.1:7077 \
    src/batch_processing/07_fraudulent_transactions_predictions/solution.py
```

Because the app does **not** hard-code `.master(...)`, the master comes entirely
from `--master`, so the job runs on the cluster. The driver runs in the default
**client** deploy mode (driver on your machine, executor on the worker).

**4) Verify it ran on the cluster**

The app logs how it maps onto Spark components, e.g.:

```
Master           : spark://127.0.0.1:7077
Application id   : app-2026...-0000
Executor added: app-.../0 on worker ... with 14 core(s)
Registered executor ... with ID 0
```

`app-...` (not `local-...`) and a separately-registered executor confirm a real
cluster run. The completed run also appears under **Completed Applications** at
http://localhost:8080.

**5) Stop the cluster**

```bash
"$SPARK_HOME/sbin/stop-worker.sh"
"$SPARK_HOME/sbin/stop-master.sh"
```

## `solution.py` — the RDD pipeline

| Step | RDD operation |
| --- | --- |
| read | `sc.textFile(Fraud.csv)` → one string per line |
| parse | `.map(parse_line)` → `(account, amount)`; header / bad rows → `None`, then `.filter(...is not None)` |
| **Step 1** | `.filter(amount > 10_000)` |
| group | `.map(acc → (acc, (amount, 1)))` then `.reduceByKey((s,c)+(s,c))` → `(acc, (sum, count))`, then `.cache()` |
| **Step 2** | `.mapValues(sum / count)` → `(acc, average)` |
| overall avg | from the cached totals: `.map(_[1]).reduce(...)` → `total_sum / total_count` |
| **Step 3** | `.filter(average >= 2 * overall_avg)` |
| write | `.toDF(["account","avg_amount"]).write.csv(..., header=True)` |

Why `reduceByKey` (not `groupByKey`): it combines `(sum, count)` **map-side**
before the shuffle, so only one small pair per key crosses the network — the
right pattern for an average at this scale. The grouped totals are `cache()`d
because they feed three actions (transaction count, overall average, final
write), so the 471 MB file is read **once**.

`main()` wraps the pipeline in `try/except/finally`: it logs the full traceback
and re-raises on error, and always calls `spark.stop()`.

## Acceptance criteria — status

| # | Criterion | Status |
| --- | --- | --- |
| 1 | Reads transaction data from the input CSV | ✅ `sc.textFile(Fraud.csv)` |
| 2 | Filtered transactions grouped by account number | ✅ `reduceByKey` on `nameOrig` |
| 3 | Writes account numbers + average amounts to output CSV | ✅ `data/output/flagged_accounts/part-*.csv` (`account,avg_amount`) |
| — | Processed with the **RDD** API | ✅ `textFile`/`map`/`filter`/`reduceByKey`/`mapValues`/`reduce` |
| — | Run in **cluster mode** | ✅ `spark-submit --master spark://127.0.0.1:7077`, separate executor |

## Implementation notes

- **RDD, by requirement.** The task asks specifically for the RDD API, so the
  whole pipeline is `textFile`/`map`/`filter`/`reduceByKey`/`mapValues`/`reduce`.
  Only the final, small result is turned into a 2-column DataFrame to emit a
  CSV **with a header** (`account,avg_amount`) — output serialization, not
  processing.
- **Threshold is strict (`>` $10 000).** Matches the wording "exceeding".
- **Loopback binding.** All daemons and the driver use `SPARK_LOCAL_IP=127.0.0.1`;
  on macOS, binding to a LAN IP can cause intermittent block-transfer failures.
- **Version match.** The Spark distribution (master/worker) and the driver's
  `pyspark` must be the same version (4.1.2), otherwise the handshake fails.
- **Data** (`data/input`, `data/output`) and **logs** are gitignored.
