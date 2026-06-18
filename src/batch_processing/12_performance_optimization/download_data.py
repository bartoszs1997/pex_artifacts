"""Download the Food Orders dataset from Kaggle.

Dataset:
    reenapinto/food-order
    (referenced by the task as the "Food Orders" dataset)

Authentication:
    The Kaggle Python client reads credentials automatically from the
    ~/.kaggle/ directory. On this machine the credential is a personal
    access token stored at:

        ~/.kaggle/access_token   (chmod 600, owner read/write only)

    No username, key, or token is ever hard-coded in this file or
    committed to the repository. The client reads the token from disk
    when api.authenticate() is called.

Output:
    Writes three files into ./data/input (gitignored via **/data/input/):
      * food_orders_raw.csv       - the raw Kaggle download (~1,900 rows)
      * food_orders_large.csv     - scaled benchmark data for the baseline job
      * food_orders_large.parquet - the same data as Parquet for the optimized job

Usage:
    uv run python download_data.py
    (run from src/batch_processing/12_performance_optimization directory)
"""

import logging
import shutil
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("download")

DATASET = "reenapinto/food-order"
DATA_DIR = Path(__file__).resolve().parent / "data/input"
RAW_CSV = DATA_DIR / "food_orders_raw.csv"
CSV_OUT = DATA_DIR / "food_orders_large.csv"
PARQUET_OUT = DATA_DIR / "food_orders_large.parquet"

# A performance benchmark needs real data volume. The raw Kaggle file is only
# ~1,900 rows (120 KB) - far too small for any Spark optimization to matter,
# because the whole runtime is dominated by fixed startup overhead. We inflate
# the data to TARGET_ROWS so that serialization, file format, partitioning and
# shuffle costs become the dominant factor - exactly what the optimized job is
# designed to improve.
TARGET_ROWS = 1_000_000


def download() -> None:
    """Authenticate with Kaggle and download the dataset into DATA_DIR."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Imported inside the function (not at module top) so this file can be
    # imported without Kaggle credentials being present, e.g. during tests.
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()  # reads ~/.kaggle/access_token (or kaggle.json)

    log.info(f"Downloading {DATASET} -> {DATA_DIR}")
    api.dataset_download_files(DATASET, path=str(DATA_DIR), unzip=True)

    # Keep the raw download under a stable name; scale() turns it into the
    # benchmark-sized CSV + Parquet that the Spark jobs actually read.
    source_csv = DATA_DIR / "Resume_food_order_3.csv"
    if source_csv.exists():
        source_csv.replace(RAW_CSV)
        log.info("food_orders_raw.csv downloaded")
    else:
        for f in sorted(DATA_DIR.glob("*")):
            if f.is_file():
                log.info(f"{f.name} file downloaded")

    log.info("Downloading Done.")


def scale() -> None:
    """Inflate the tiny raw dataset into a benchmark-sized CSV + Parquet.

    Writes two files used by the Spark jobs:
      * food_orders_large.csv     - read by the baseline (CSV, 3x, no cache)
      * food_orders_large.parquet - read by the optimized job (columnar)

    Building the Parquet here (once, at prep time) keeps the comparison fair:
    the optimized job never pays a CSV->Parquet conversion cost in its timer.
    """
    base = pd.read_csv(RAW_CSV)
    n_base = len(base)
    factor = max(1, -(-TARGET_ROWS // n_base))  # ceil division
    log.info(f"Scaling {n_base:,} rows x{factor} -> ~{n_base * factor:,} rows")

    big = pd.concat([base] * factor, ignore_index=True)
    big["order_id"] = range(1, len(big) + 1)  # unique id per row

    big.to_csv(CSV_OUT, index=False)
    log.info(
        f"Wrote {CSV_OUT.name} "
        f"({CSV_OUT.stat().st_size / 1e6:.0f} MB, {len(big):,} rows)"
    )

    # Replace any previous Parquet (a single file, or a Spark-written dir).
    if PARQUET_OUT.is_dir():
        shutil.rmtree(PARQUET_OUT)
    elif PARQUET_OUT.exists():
        PARQUET_OUT.unlink()
    big.to_parquet(PARQUET_OUT, index=False)
    log.info(f"Wrote {PARQUET_OUT.name} ({PARQUET_OUT.stat().st_size / 1e6:.0f} MB)")


if __name__ == "__main__":
    download()
    scale()

