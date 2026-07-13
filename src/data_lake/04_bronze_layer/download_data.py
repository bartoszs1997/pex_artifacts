"""Download the dataset for task 04 (bronze layer Delta pipeline).

Dataset:
    wjia26/big-tech-companies-tweet-sentiment  ("Big Tech Companies")

Authentication:
    The Kaggle client reads credentials automatically from ~/.kaggle/
    (on this machine: ~/.kaggle/access_token, chmod 600). Nothing is
    hard-coded and nothing is committed.

Usage:
    uv run python src/data_lake/04_bronze_layer/download_data.py
"""

import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("download")

DATASET = "wjia26/big-tech-companies-tweet-sentiment"
DATA_DIR = Path(__file__).resolve().parent / "data/input"


def download() -> None:
    """Download the dataset into this task's data/input/ directory."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Imported inside the function so this file can be imported without creds.
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()  # reads ~/.kaggle/access_token (or kaggle.json)

    log.info(f"Downloading {DATASET} -> {DATA_DIR}")
    api.dataset_download_files(DATASET, path=str(DATA_DIR), unzip=True)
    log.info("Downloading Done.")


if __name__ == "__main__":
    download()
