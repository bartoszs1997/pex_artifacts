"""Download the Food Orders dataset from Kaggle.

Dataset:
    reenapinto/food-order

Authentication:
    The Kaggle Python client reads credentials automatically from the
    ~/.kaggle/ directory. On this machine the credential is a personal
    access token stored at:

        ~/.kaggle/access_token   (chmod 600, owner read/write only)

    No username, key, or token is ever hard-coded in this file or
    committed to the repository. The client reads the token from disk
    when api.authenticate() is called.

Output:
    The dataset is downloaded and unzipped into ./data/input next to this
    script. That folder is gitignored (see .gitignore: **/data/input/),
    so the data files never enter git.

Usage:
    uv run python src/batch_processing/08_food_orders/download_data.py
"""

import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("download")

DATASET = "reenapinto/food-order"
DATA_DIR = Path(__file__).resolve().parent / "data/input"


def download() -> None:
    """Authenticate with Kaggle and download the dataset into DATA_DIR."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()

    log.info(f"Downloading {DATASET} -> {DATA_DIR}")
    api.dataset_download_files(DATASET, path=str(DATA_DIR), unzip=True)

    for f in sorted(DATA_DIR.glob("*")):
        if f.is_file():
            log.info(f"{f.name} file downloaded")
    log.info("Downloading Done.")


if __name__ == "__main__":
    download()
