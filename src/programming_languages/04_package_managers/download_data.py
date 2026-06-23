"""Download the user_data dataset from Kaggle.

Dataset: sauravkumaragarwal/user-datacsv
Source:  https://www.kaggle.com/datasets/sauravkumaragarwal/user-datacsv
File:    final_user_scores.csv (~109K rows)

Usage:
    uv run python src/programming_languages/04_package_managers/download_data.py
"""

import logging
import sys
from pathlib import Path

from kaggle.api.kaggle_api_extended import KaggleApi

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR = SCRIPT_DIR / "data" / "input"
OUTPUT_FILE = INPUT_DIR / "final_user_scores.csv"

DATASET = "sauravkumaragarwal/user-datacsv"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("download_data")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    """Download dataset from Kaggle API."""
    if OUTPUT_FILE.exists():
        log.info("File already exists: %s — skipping download.", OUTPUT_FILE)
        return 0

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Downloading dataset '%s' from Kaggle...", DATASET)

    try:
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(DATASET, path=str(INPUT_DIR), unzip=True)

        if OUTPUT_FILE.exists():
            size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
            log.info("Downloaded %.1f MB -> %s", size_mb, OUTPUT_FILE)
        else:
            log.error("Download completed but expected file not found: %s", OUTPUT_FILE)
            return 1
    except Exception:
        log.exception("Failed to download dataset from Kaggle.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
