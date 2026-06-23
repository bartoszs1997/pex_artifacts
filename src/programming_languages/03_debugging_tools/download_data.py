"""Download the Health Insurance Marketplace Rate.csv from Kaggle.

Dataset: hhs/health-insurance-marketplace
File:    Rate.csv (~120 MB uncompressed)

Requires Kaggle API token configured (~/.kaggle/kaggle.json).

Usage:
    uv run python src/programming_languages/03_debugging_tools/download_data.py
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

DATASET = "hhs/health-insurance-marketplace"
TARGET_FILE = "Rate.csv"

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
    """Download Rate.csv from Kaggle if not already present."""
    output_file = INPUT_DIR / TARGET_FILE

    if output_file.exists():
        log.info("File already exists: %s — skipping download.", output_file)
        return 0

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Downloading %s from Kaggle dataset '%s'...", TARGET_FILE, DATASET)

    try:
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_file(
            DATASET, TARGET_FILE, path=str(INPUT_DIR), force=True
        )

        # Kaggle may download as .zip — handle both cases
        zip_file = INPUT_DIR / (TARGET_FILE + ".zip")
        if zip_file.exists():
            import zipfile

            with zipfile.ZipFile(zip_file, "r") as zf:
                zf.extractall(INPUT_DIR)
            zip_file.unlink()
            log.info("Extracted %s from zip archive.", TARGET_FILE)

        if output_file.exists():
            size_mb = output_file.stat().st_size / (1024 * 1024)
            log.info("Downloaded %.1f MB -> %s", size_mb, output_file)
        else:
            log.error("Download completed but %s not found.", TARGET_FILE)
            return 1

    except Exception:
        log.exception("Failed to download %s", TARGET_FILE)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
