"""Download the TV Series dataset from Kaggle.

Requires Kaggle API token in ~/.kaggle/access_token (or kaggle.json).
Downloads to the local data/ directory (gitignored).

Dataset: bourdier/all-tv-series-details-dataset
"""

import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("download")

DATASET = "bourdier/all-tv-series-details-dataset"
DATA_DIR = Path(__file__).resolve().parent / "data"


def download() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    log.info("Downloading %s -> %s", DATASET, DATA_DIR)
    api.dataset_download_files(DATASET, path=str(DATA_DIR), unzip=True)

    for f in DATA_DIR.glob("*"):
        if f.is_file():
            log.info("  %s (%.1f MB)", f.name, f.stat().st_size / 1024 / 1024)
    log.info("Done.")


if __name__ == "__main__":
    download()
