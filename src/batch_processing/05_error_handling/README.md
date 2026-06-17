# Error Handling — Batch Processing (Spark logging, alerting & recovery)

Spark batch application over the **TV Series Dataset** that demonstrates a full
error-handling and observability layer. It is based on the Phase-1 TV Series
analysis (`01_tv_series/basic_solution.py` — the same three queries) and wraps
it in exception handling, level-routed logging, alerting, runtime metrics, and a
recoverable simulated failure.

## Task

> NEBo task — *Handle errors*: handle and log errors during Spark batch
> processing; implement exception handling, logging, and alerting so errors are
> captured and acted upon; capture run-time information.

The four "to handle errors" points, and where each is implemented:

| # | Requirement | Where in `solution.py` |
| --- | --- | --- |
| 1 | Print **all** logs in the terminal at **DEBUG** level | `console_handler.setLevel(DEBUG)` + `log.setLevel(DEBUG)` |
| 2 | Save **WARNING and above** in the log file | `file_handler.setLevel(WARNING)` |
| 3 | Create a specific message stored at **WARNING** level | `check_data_quality()` → `log.warning(...)` |
| 4 | Apply fixes at the **ERROR** level | `read_data()` → `log.error` + `alert` + fallback |

## How the level routing works (the key idea)

A log record passes **two** gates: first the **logger's** level, then each
**handler's** level. So the logger is opened fully and each destination is
filtered independently:

```
log.setLevel(DEBUG)              # gate 1: let everything through
  ├─ console_handler @ DEBUG     # terminal keeps DEBUG, INFO, WARNING, …  (req 1)
  └─ file_handler    @ WARNING   # file keeps only WARNING, ERROR, CRITICAL (req 2)
```

If `log.setLevel(...)` is left at the default, the logger's effective level is
`WARNING` and **every `log.info()` / `log.debug()` is dropped before it ever
reaches a handler** — which is exactly the defect this task asks you to fix.

## What it does

Loads the TV Series JSON, logs runtime context, checks data quality, then runs
the three Phase-1 queries (canceled creators, popular countries, short series),
showing each result and writing it to CSV.

| Signal | Captured by | Example (real run) |
| --- | --- | --- |
| Execution time | `time.perf_counter()` around `main` | `Total execution time: 9.51s` |
| Resource utilization | stdlib `resource.getrusage` (driver process) | `peak memory 103 MB, CPU user 0.2s / sys 0.1s` |
| Data statistics | `count()` / `len(columns)` / `getNumPartitions()` | `152970 rows x 22 columns`; per-query row counts |
| Data-quality warning | `check_data_quality()` | `634/152970 rows have a NULL 'number_of_episodes'` |

## Layout

```
05_error_handling/
├── solution.py        # Spark app: queries + logging/alerting/recovery/metrics
├── data/              # gitignored (output only)
│   └── output/        # canceled_creators / popular_countries / short_series (CSV)
└── logs/              # gitignored
    └── error_handling.log   # WARNING and above only
```

> **No `download_data.py` / `data/input` here on purpose.** This module reuses
> the TV Series dataset already downloaded for module 01
> (`../01_tv_series/data/input/tvs.json`) instead of duplicating a 151 MB file.
> If that file is missing, run `01_tv_series/download_data.py` first, or pass
> `--path` to point at the JSON elsewhere.

## Prerequisites

- **Python** managed by `uv` (`uv sync` at the repo root).
- **Java 17** on `PATH` (required by PySpark 4.x):
  ```bash
  export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
  ```
- The **TV Series dataset** present at `01_tv_series/data/input/tvs.json`
  (see module 01), or a path supplied via `--path`.

## Run

All commands are run from the repo root.

```bash
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"

# normal run — queries succeed
uv run python src/batch_processing/05_error_handling/solution.py

# demonstrate the error-handling path — simulated failure that recovers
uv run python src/batch_processing/05_error_handling/solution.py --simulate-error
```

## Demonstrating the error handling (`--simulate-error`)

`--simulate-error` points the primary read at a file that does not exist. The
failure is **caught, logged at ERROR, alerted (CRITICAL), then fixed** by
falling back to the known-good dataset, so the run still completes. Terminal
sequence:

```
INFO     Reading data from .../05_error_handling/data/input/does_not_exist.json...
ERROR    Failed to read data ...: [PATH_NOT_FOUND] Path does not exist ... SQLSTATE: 42K03
CRITICAL [ALERT] Primary data read failed (does_not_exist.json); attempting recovery.
WARNING  Recovery: falling back to known-good dataset at .../01_tv_series/data/input/tvs.json
INFO     Loaded TV Series data: 152970 rows x 22 columns
```

The log file then contains **only** the WARNING+ records (no INFO/DEBUG noise):

```
ERROR    Failed to read data ...: [PATH_NOT_FOUND] Path does not exist ...
CRITICAL [ALERT] Primary data read failed (does_not_exist.json); attempting recovery.
WARNING  Recovery: falling back to known-good dataset ...
WARNING  Data quality: 634/152970 rows have a NULL 'number_of_episodes' ...
```

## Implementation notes

- **Alerting.** `alert()` logs at CRITICAL with an `[ALERT]` marker so it stands
  out and (being ≥ WARNING) is persisted to the file. In production this is the
  seam where you would page on-call (Slack / PagerDuty / email / SNS).
- **Recovery vs. re-raise.** `read_data()` recovers from a failed *primary*
  read by falling back to the known-good dataset. If the fallback itself is the
  one that fails (nothing left to recover to), it re-raises. The outer
  `try/except/finally` in `main()` is the last line of defence: it logs the full
  traceback (`log.exception`), alerts, and **re-raises** (no silent masking,
  non-zero exit); `finally` always reports timing/resources and stops Spark.
- **Resource scope.** `resource.getrusage(RUSAGE_SELF)` measures the **driver
  Python process** (peak RSS + CPU). The Spark JVM runs as a separate process,
  so its memory is observed in the Spark UI rather than here — this number is
  the driver footprint, captured with no extra dependencies. (`ru_maxrss` is in
  bytes on macOS, kilobytes on Linux; the code handles both.)
- **Loopback networking.** The driver is pinned to `127.0.0.1`
  (`spark.driver.host` / `spark.driver.bindAddress`) to avoid the macOS LAN-IP
  block-transfer failures. Environment workaround, not a Spark requirement.
- **Data** (`data/output`) and **logs** are gitignored and never enter git.

## Acceptance criteria — status

| # | Criterion | Status |
| --- | --- | --- |
| 1 | Error-handling mechanisms catch and handle errors during execution | ✅ `try/except` in `read_data` + `main`; recovery fallback |
| 2 | Error messages logged to a log file | ✅ `logs/error_handling.log` (WARNING+; ERROR/CRITICAL included) |
| 3 | Run-time info captured (time, resources, data statistics) | ✅ execution time + `getrusage` + row/column/partition counts |
| 4 | Simulated error is handled, logged, and alerted | ✅ `--simulate-error` → ERROR + `[ALERT]` + recovery |
