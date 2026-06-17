# English Words — Batch Processing (Spark Standalone cluster)

Spark batch application over the **English words** dataset, run on a **Spark
Standalone cluster** and submitted with `spark-submit`. It reads a plain-text
word list, answers two counting questions, and writes a transformed dataset to
CSV.

## Task

Run a Spark job in **cluster mode** using the **Standalone** deploy mode: set up
a cluster (master + worker) and submit a Spark application to it that reads the
dataset and:

- **Q1:** counts the total number of words that start with `abs`.
- **Q2:** counts the total number of words whose third letter is `o`.
- **Q3:** in every word ending with `s`, replaces the combination `ou` with `uou`.

## Results

| Question | Result |
| --- | --- |
| Q1 — words starting with `abs` | **267** |
| Q2 — words whose third letter is `o` | **29 214** |
| Q3 — words ending in `s` with `ou`→`uou` | **10 836** changed (full list written to CSV) |

(These were cross-checked with an independent pure-Python pass over the same
file — identical numbers.)

## Dataset

[`dwyl/english-words`](https://github.com/dwyl/english-words) — a plain-text file
with one token per line (~466 550 lines, ~4.9 MB). It is fetched directly over
HTTPS from the raw GitHub URL; no Kaggle account or API token is needed.

> The list also contains non-alphabetic tokens (numbers, hyphenated forms such
> as `10-point`). We keep them as-is, since the task points at this exact list;
> all comparisons are case-insensitive (`lower(...)`), because the file mixes
> upper- and lower-case entries.

## Layout

```
03_english_words/
├── download_data.py   # fetch words.txt from GitHub (HTTPS) -> data/input/
├── solution.py        # Spark app: Q1/Q2 counts + Q3 transform -> CSV
├── data/              # gitignored
│   ├── input/words.txt
│   └── output/modified_words/   # Q3 result as CSV (word,new_word)
└── logs/              # gitignored
    └── words.log
```

## Prerequisites

- **Python** managed by `uv` (`uv sync` at the repo root).
- **Java 17** on `PATH` (required by PySpark 4.x):
  ```bash
  export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
  export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
  ```
- **A full Spark distribution** for the cluster scripts (see the note below on
  why the pip/uv `pyspark` wheel is not enough). Download the version that
  matches the `pyspark` pinned in `pyproject.toml` (here: 4.1.2):
  ```bash
  cd ~
  curl -fLO https://archive.apache.org/dist/spark/spark-4.1.2/spark-4.1.2-bin-hadoop3.tgz
  tar xzf spark-4.1.2-bin-hadoop3.tgz
  export SPARK_HOME="$HOME/spark-4.1.2-bin-hadoop3"
  ```

> **Why a full distribution?** The `pyspark` wheel installed by pip/uv ships
> `bin/spark-submit` but **not** the Standalone cluster scripts
> (`sbin/start-master.sh`, `sbin/start-worker.sh`). To actually *stand up* a
> cluster you need the full Apache Spark distribution, which bundles `sbin/`.
> The distribution version must match the `pyspark` version used by the driver.

## Run — Standalone cluster mode (step by step)

All app commands are run from the repo root. Keep `SPARK_LOCAL_IP=127.0.0.1` so
every component binds to loopback (avoids the macOS LAN-IP networking issues).

### 0) Download the dataset

```bash
uv run python src/batch_processing/03_english_words/download_data.py
```

### 1) Start the master

```bash
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export SPARK_HOME="$HOME/spark-4.1.2-bin-hadoop3"
export SPARK_MASTER_HOST=localhost
export SPARK_LOCAL_IP=127.0.0.1

"$SPARK_HOME/sbin/start-master.sh"
```

This starts the master at **`spark://localhost:7077`** with a web UI at
**http://localhost:8080**. Confirm in the master log
(`$SPARK_HOME/logs/*master*.out`):

```
Starting Spark master at spark://localhost:7077
I have been elected leader! New state: ALIVE
```

### 2) Start a worker and register it with the master

```bash
export SPARK_LOCAL_IP=127.0.0.1
"$SPARK_HOME/sbin/start-worker.sh" spark://localhost:7077
```

The worker UI is at **http://localhost:8081**. Confirm in the worker log:

```
Starting Spark worker 127.0.0.1:xxxxx with 14 cores, 47.0 GiB RAM
Successfully registered with master spark://localhost:7077
```

At this point the **cluster is up**: one master + one worker (visible under
"Workers" in the :8080 UI).

### 3) Submit the application with spark-submit

Point `PYSPARK_PYTHON` at this project's venv so the executors use the same
interpreter (with `pyspark` available), then submit against the master URL:

```bash
export PYSPARK_PYTHON="$(pwd)/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$(pwd)/.venv/bin/python"

"$SPARK_HOME/bin/spark-submit" \
    --master spark://localhost:7077 \
    --name "WordsAnalysis" \
    src/batch_processing/03_english_words/solution.py
```

Because the app does **not** hard-code `.master(...)`, the master comes entirely
from `--master`, so the cluster runs the job. The driver runs in the default
**client** deploy mode (driver on your machine, executors on the worker).

### 4) Verify the job ran on the cluster and finished

In the submit output you should see the app connect to the standalone master and
register an executor:

```
Connecting to master spark://localhost:7077...
Connected to Spark cluster with app ID app-2026...-0000
Registered executor ... (127.0.0.1:xxxxx) with ID 0
```

The Q1/Q2/Q3 results are logged, a sample of changed words is printed, and the
full Q3 output is written to `data/output/modified_words/` (look for `_SUCCESS`
+ `part-*.csv`). In the **master** log the application lifecycle ends cleanly:

```
Registering app WordsAnalysis
Launching executor app-.../0 on worker worker-...
Removing app app-...-0000            # driver called spark.stop() -> FINISHED
```

The completed run also appears under **Completed Applications** at
http://localhost:8080.

### 5) Stop the cluster

```bash
"$SPARK_HOME/sbin/stop-worker.sh"
"$SPARK_HOME/sbin/stop-master.sh"
```

## `solution.py`

| Step | Logic |
| --- | --- |
| read | `spark.read.text(words.txt)` → one row per line (column `value`) |
| explode | `split` on whitespace + `explode` → one row per word (column `word`) |
| Q1 | `filter(lower(word).startswith("abs")).count()` |
| Q2 | `filter(lower(substring(word, 3, 1)) == "o").count()` (Spark `substring` is 1-indexed) |
| Q3 | `withColumn("new_word", when(endswith "s", regexp_replace "(?i)ou"→"uou"))` then write CSV |

`main()` wraps the pipeline in `try/except/finally`: it logs the full traceback
and re-raises on error, and always calls `spark.stop()`.

## Acceptance criteria — status

| # | Criterion | Status |
| --- | --- | --- |
| 1 | Spark cluster in Standalone deploy mode | ✅ master + worker on `spark://localhost:7077` |
| 2 | Spark application written | ✅ `solution.py` |
| 3 | Packaged into a **JAR** | ⚠️ N/A for Python — see below |
| 4 | Submitted via `spark-submit` | ✅ `spark-submit --master spark://localhost:7077` |
| 5 | Expected output produced, job completed | ✅ 267 / 29 214 / 10 836, app `FINISHED` |

### Note on criterion #3 (JAR packaging)

A **JAR** (Java ARchive) is a JVM artifact: it packages compiled `.class`
bytecode. **Python code does not compile to a JAR**, so for a PySpark
application this criterion does not apply literally — this track is a Python
specialization, and the references' "Code Example in Scala" reflects the JVM
path the JAR criterion was written for.

The Python equivalent of "packaging the application" is the application file
itself (`solution.py`), submitted with `spark-submit`. When an app has
third-party dependencies, they are bundled into a `.zip`/`.egg` and shipped with
`--py-files` — the conceptual analogue of a JAR. Our app only uses PySpark
(already present on the cluster), so a single `.py` is the complete deliverable.

`--deploy-mode cluster` is also not supported for Python applications on a
Standalone cluster (only JVM apps can have their driver run on a worker), so the
job runs in the standard **client** mode, which is fully "on the cluster":
the executors run on the worker.

## Implementation notes

- **Loopback binding.** All daemons and the driver use `SPARK_LOCAL_IP=127.0.0.1`
  (master also `SPARK_MASTER_HOST=localhost`). On macOS, binding to a LAN IP can
  cause intermittent block-transfer failures; loopback is the reliable choice
  for a single-node cluster.
- **Version match.** The Spark distribution (master/worker) and the driver's
  `pyspark` must be the same version (4.1.2 here), otherwise the handshake fails.
- **Data** (`data/input`, `data/output`) and **logs** are gitignored.
