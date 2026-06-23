"""Download the Instacart orders dataset from GitHub.

Source: https://github.com/billyrohh/instacart_dataset
File:   orders.csv (approx. 3.4M rows)

Columns: order_id, user_id, eval_set, order_number, order_dow,
         order_hour_of_day, days_since_prior_order

Usage:
    uv run python src/programming_languages/02_standard_library/download_data.py
"""

import logging
import ssl
import sys
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR = SCRIPT_DIR / "data" / "input"
OUTPUT_FILE = INPUT_DIR / "orders.csv"

RAW_URL = (
    "https://raw.githubusercontent.com/billyrohh/instacart_dataset/master/orders.csv"
)

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
    """Download orders.csv if not already present."""
    if OUTPUT_FILE.exists():
        log.info("File already exists: %s — skipping download.", OUTPUT_FILE)
        return 0

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Downloading orders.csv from GitHub...")

    try:
        # Create SSL context with certifi certificates
        import certifi

        ssl_context = ssl.create_default_context(cafile=certifi.where())
        req = urllib.request.Request(RAW_URL)
        with urllib.request.urlopen(req, context=ssl_context) as response:
            data = response.read()

        OUTPUT_FILE.write_bytes(data)
        size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
        log.info("Downloaded %.1f MB -> %s", size_mb, OUTPUT_FILE)
    except Exception:
        log.exception("Failed to download orders.csv")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
