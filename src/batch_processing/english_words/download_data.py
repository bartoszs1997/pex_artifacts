"""Download the English Words dataset.

Dataset:
    dwyl/english-words  (referenced by the task as "English words")
    https://github.com/dwyl/english-words

Source file:
    The raw word list is fetched directly over HTTPS from GitHub:
        https://raw.githubusercontent.com/dwyl/english-words/master/words.txt
    It is a plain-text file with one token per line (~466k lines, ~4.9 MB),
    including some non-alphabetic entries (numbers, hyphenated forms) which we
    keep as-is, since the task points at this exact list.

    No Kaggle account or API token is needed: it is a public raw file, so a
    plain HTTPS GET is enough.

Output:
    Saved to ./data/input/words.txt next to this script. That folder is
    gitignored (see .gitignore: **/data/input/), so the data never enters git.

Usage:
    uv run python src/batch_processing/english_words/download_data.py
"""

import logging
import shutil
import ssl
import urllib.request
from pathlib import Path

import certifi

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("download")

WORDS_URL = "https://raw.githubusercontent.com/dwyl/english-words/master/words.txt"
DATA_DIR = Path(__file__).resolve().parent / "data" / "input"
WORDS_FILE = DATA_DIR / "words.txt"


def download() -> None:
    """Fetch the English words list over HTTPS into WORDS_FILE."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    log.info(f"Downloading {WORDS_URL} -> {WORDS_FILE}")
    # macOS framework Python does not use the system trust store, so point TLS
    # verification at certifi's CA bundle (avoids CERTIFICATE_VERIFY_FAILED).
    context = ssl.create_default_context(cafile=certifi.where())
    request = urllib.request.Request(WORDS_URL, headers={"User-Agent": "pex-artifacts/1.0"})
    with urllib.request.urlopen(request, context=context) as response, WORDS_FILE.open("wb") as out_file:
        shutil.copyfileobj(response, out_file)

    size_mb = WORDS_FILE.stat().st_size / 1_000_000
    with WORDS_FILE.open(encoding="utf-8") as f:
        line_count = sum(1 for _ in f)
    log.info(f"Saved {WORDS_FILE.name} ({size_mb:.1f} MB, {line_count:,} lines)")
    log.info("Downloading Done.")


if __name__ == "__main__":
    download()
