"""Download the dataset for task 01 (ingest data into a data lake).

Dataset:
    ryanholbrook/dl-course-data  ("DL Course Data")

Authentication:
    The Kaggle client reads credentials automatically from ~/.kaggle/
    (on this machine: ~/.kaggle/access_token, chmod 600). Nothing is
    hard-coded and nothing is committed.

CSV and JSON:
    The task must ingest BOTH CSV and JSON, but this dataset ships only CSV
    files. So after downloading we write a JSON copy of housing.csv. That gives
    the batch pipeline a real CSV source and a real JSON source.

Usage:
    uv run python src/data_lake/01_ingest_data/download_data.py
"""

import csv
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("download")

DATASET = "ryanholbrook/dl-course-data"
DATA_DIR = Path(__file__).resolve().parent / "data/input"


def download() -> None:
    """Download the dataset, then emit a JSON copy of housing.csv."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Imported inside the function so this file can be imported without creds.
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()  # reads ~/.kaggle/access_token (or kaggle.json)

    log.info(f"Downloading {DATASET} -> {DATA_DIR}")
    api.dataset_download_files(DATASET, path=str(DATA_DIR), unzip=True)

    _write_json_copy()
    log.info("Downloading Done.")


def _write_json_copy() -> None:
    """Turn housing.csv into newline-delimited JSON (one object per line)."""
    csv_path = DATA_DIR / "housing.csv"
    json_path = DATA_DIR / "housing.json"

    with csv_path.open(newline="") as fin, json_path.open("w") as fout:
        rows = 0
        for row in csv.DictReader(fin):
            fout.write(json.dumps(row) + "\n")
            rows += 1

    log.info(f"housing.json written ({rows} rows)")


if __name__ == "__main__":
    download()
