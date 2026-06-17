# Disease Symptoms — Batch Processing (Spark distributed collections)

Spark batch application over the **Disease Symptoms and Patient Profile**
dataset. It loads a CSV into a Spark **DataFrame** (a distributed collection),
applies filtering and aggregation transformations, and answers three
analytical questions.

## Task

> NEBo task: *Use distributed collections in Spark* — load a dataset into an
> RDD/DataFrame, apply transformations, and run computations using Spark's
> relational operations.

- **Q1:** Count the total number of 30-year-old **Males** with **Asthma**.
- **Q2:** Count the total number of **Females** with **Hyperthyroidism** and **No Fever**.
- **Q3:** For **Sinusitis** with **Cough** and **Fatigue**, is it predominant in males or females?

## Results

| Question | Result |
| --- | --- |
| Q1 — 30-year-old males with Asthma | **0** |
| Q2 — females, Hyperthyroidism, no Fever | **2** |
| Q3 — Sinusitis + Cough + Fatigue, predominant gender | **Male** |

These were cross-checked with an independent pure-pandas pass over the same CSV
— identical results.

> **Two data caveats (not bugs):**
> - **Q1 = 0** is correct. The dataset *does* contain 30-year-olds and *does*
>   contain males with Asthma — but their intersection is empty (every
>   "Age 30 + Asthma" row is Female, and the male Asthma patients are other ages).
> - **Q3** rests on a **single** Sinusitis row (one male with Cough + Fatigue,
>   zero females), so "Male" wins on a sample of n=1.

## Dataset

[`uom190346a/disease-symptoms-and-patient-profile-dataset`](https://www.kaggle.com/datasets/uom190346a/disease-symptoms-and-patient-profile-dataset)
— a single CSV, **349 rows × 10 columns**:

| Column | Values |
| --- | --- |
| `Disease` | 116 distinct (Asthma, Hyperthyroidism, Sinusitis, …) |
| `Fever`, `Cough`, `Fatigue`, `Difficulty Breathing` | `Yes` / `No` |
| `Age` | integer (19–90) |
| `Gender` | `Male` / `Female` |
| `Blood Pressure`, `Cholesterol Level` | Low / Normal / High |
| `Outcome Variable` | Positive / Negative |

The symptom flags are strings (`"Yes"`/`"No"`), so filters compare against those
literals rather than booleans.

## Layout

```
04_disease_symptoms/
├── download_data.py   # fetch the CSV from Kaggle -> data/input/
├── solution.py        # Spark app: load CSV, Q1/Q2/Q3, write Q3 summary to CSV
├── data/              # gitignored
│   ├── input/Disease_symptom_and_patient_profile_dataset.csv
│   └── output/sinusitis_by_gender/   # Q3 gender breakdown as CSV
└── logs/              # gitignored
    └── disease.log
```

## Prerequisites

- **Python** managed by `uv` (`uv sync` at the repo root).
- **Java 17** on `PATH` (required by PySpark 4.x):
  ```bash
  export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
  ```
- **Kaggle token** at `~/.kaggle/access_token` (or `kaggle.json`), `chmod 600`.

## Run

All commands are run from the repo root.

```bash
# 0) (once) sync the environment
uv sync

# 1) download the dataset -> data/input/
uv run python src/batch_processing/04_disease_symptoms/download_data.py

# 2) run the Spark app — load CSV, answer Q1-Q3, write Q3 summary to data/output/
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/batch_processing/04_disease_symptoms/solution.py
```

## `solution.py`

| Step | Function | Logic |
| --- | --- | --- |
| load | `load_dataset` | `spark.read.csv(header=True, inferSchema=True)` → DataFrame |
| Q1 | `count_30_year_old_males_with_asthma` | `filter(Disease=Asthma & Age=30 & Gender=Male).count()` |
| Q2 | `count_females_with_hyperthyroidism_no_fever` | `filter(Disease=Hyperthyroidism & Gender=Female & Fever=No).count()` |
| Q3 | `count_sinusitis_with_cough_fatigue` | `filter(Disease=Sinusitis & Cough=Yes & Fatigue=Yes).groupBy(Gender).agg(count)` |
| Q3 | `identify_predominant_gender` | `orderBy(Volume desc).first()` → winning gender |
| save | `write_to_csv` | write the Q3 gender breakdown to CSV (`mode("overwrite")`) |

`main()` wraps the pipeline in `try/except/finally`: it logs the full traceback
and re-raises on error, and always calls `spark.stop()`. Output is logged to the
console and to `logs/disease.log`.

## Acceptance criteria — status

| # | Criterion | How it is met |
| --- | --- | --- |
| 1 | Dataset loaded into a distributed collection (RDD/DataFrame) | `spark.read.csv(...)` → DataFrame |
| 2 | At least two transformations applied | `filter` (Q1/Q2/Q3) + `groupBy`/`agg` (Q3) |
| 3 | Computation / analysis on transformed data | `count()` (Q1/Q2), gender aggregation + summary CSV (Q3) |
| 4 | Runs without errors, produces expected output | exit 0, 0 tracebacks; results 0 / 2 / Male match an independent check |
| 5 | Code documented (purpose of each transformation) | module + function docstrings and Q1/Q2/Q3 comments |

## Implementation notes

- **Local mode + loopback.** The session uses `local[*]` and pins the driver to
  `127.0.0.1` (`spark.driver.host` / `spark.driver.bindAddress`) to avoid the
  intermittent macOS LAN-IP block-transfer failures.
- **Data** (`data/input`, `data/output`) and **logs** are gitignored.
