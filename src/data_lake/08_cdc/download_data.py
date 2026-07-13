"""Download the sample MySQL database for task 08 (CDC).

Source:
    classicmodels ("MySQL Sample Database") from mysqltutorial.org, a single
    SQL dump. It creates the `classicmodels` schema; the `customers` table is
    the CDC subject.

The dump is placed in data/input/ and is mounted read-only into the MySQL
container's /docker-entrypoint-initdb.d, so MySQL loads it automatically on
first startup.

Usage:
    uv run python src/data_lake/08_cdc/download_data.py
"""

import io
import logging
import zipfile
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("download")

URL = "https://www.mysqltutorial.org/wp-content/uploads/2023/10/mysqlsampledatabase.zip"
DATA_DIR = Path(__file__).resolve().parent / "data/input"
SQL_NAME = "mysqlsampledatabase.sql"


def download() -> None:
    """Download and unzip the classicmodels SQL dump into data/input/."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    log.info(f"Downloading {URL}")
    resp = requests.get(URL, timeout=60)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open(SQL_NAME) as src:
            (DATA_DIR / SQL_NAME).write_bytes(src.read())

    log.info(f"Wrote {DATA_DIR / SQL_NAME}")
    log.info("Downloading Done.")


if __name__ == "__main__":
    download()
