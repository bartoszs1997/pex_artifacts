"""Download IBM Transactions for Anti-Money Laundering dataset from Kaggle.

Source: https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml
File:   HI-Large_Trans.csv

Requirements:
    - Kaggle API credentials (~/.kaggle/kaggle.json)
    - kaggle package (already in pyproject.toml)

Output: data/input/HI-Large_Trans.csv

Usage:
    uv run python src/programming_languages/05_unit_tests/download_data.py
"""

from pathlib import Path

from kaggle import KaggleApi

SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR = SCRIPT_DIR / "data" / "input"
DATASET = "ealtman2019/ibm-transactions-for-anti-money-laundering-aml"
FILE_NAME = "HI-Large_Trans.csv"
EXPECTED_FILE = INPUT_DIR / FILE_NAME


def main() -> None:
    """Download the AML dataset via Kaggle API."""
    INPUT_DIR.mkdir(parents=True, exist_ok=True)

    if EXPECTED_FILE.exists():
        size_mb = EXPECTED_FILE.stat().st_size / (1024 * 1024)
        print(f"File already exists: {EXPECTED_FILE} ({size_mb:.1f} MB). Skipping.")
        return

    print(f"Downloading file: {FILE_NAME} from {DATASET}")
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_file(DATASET, file_name=FILE_NAME, path=str(INPUT_DIR))

    # Kaggle may download as .zip — unzip if needed
    zip_path = INPUT_DIR / f"{FILE_NAME}.zip"
    if zip_path.exists():
        import zipfile
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(INPUT_DIR)
        zip_path.unlink()

    if EXPECTED_FILE.exists():
        size_mb = EXPECTED_FILE.stat().st_size / (1024 * 1024)
        print(f"Downloaded {size_mb:.1f} MB -> {EXPECTED_FILE}")
    else:
        print(f"ERROR: Expected file not found at {EXPECTED_FILE}")


if __name__ == "__main__":
    main()
