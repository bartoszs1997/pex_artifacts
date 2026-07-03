"""Download IBM AML Transactions dataset from Kaggle.

Source: https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml
File:   HI-Large_Trans.csv (22.3M rows, 11 columns)

Output: data/input/HI-Large_Trans.csv

Usage:
    uv run python src/programming_languages/12_concurrency/download_data.py
"""

from pathlib import Path

from kaggle import KaggleApi

SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR = SCRIPT_DIR / "data" / "input"
DATASET = "ealtman2019/ibm-transactions-for-anti-money-laundering-aml"
TARGET_FILE = "HI-Large_Trans.csv"


def main() -> None:
    """Download the IBM AML dataset via Kaggle API."""
    INPUT_DIR.mkdir(parents=True, exist_ok=True)

    if (INPUT_DIR / TARGET_FILE).exists():
        print(f"File already exists: {TARGET_FILE}. Skipping.")
        return

    print(f"Downloading {TARGET_FILE} from {DATASET}")
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_file(
        DATASET, file_name=TARGET_FILE, path=str(INPUT_DIR), force=True
    )

    # Unzip if needed
    zip_path = INPUT_DIR / f"{TARGET_FILE}.zip"
    if zip_path.exists():
        import zipfile
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(INPUT_DIR)
        zip_path.unlink()

    csv_path = INPUT_DIR / TARGET_FILE
    if csv_path.exists():
        print(f"Downloaded {csv_path.stat().st_size / (1024**3):.2f} GB -> {csv_path}")
    else:
        print("ERROR: File not found after download.")


if __name__ == "__main__":
    main()
