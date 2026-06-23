"""Download Employee Salaries dataset from Kaggle.

Source: https://www.kaggle.com/datasets/inductiveanks/employee-salaries-for-different-job-roles
File:   ds_salaries.csv

Output: data/input/ds_salaries.csv

Usage:
    uv run python src/programming_languages/08_design_patterns/download_data.py
"""

from pathlib import Path

from kaggle import KaggleApi

SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR = SCRIPT_DIR / "data" / "input"
DATASET = "inductiveanks/employee-salaries-for-different-job-roles"


def main() -> None:
    """Download the Employee Salaries dataset via Kaggle API."""
    INPUT_DIR.mkdir(parents=True, exist_ok=True)

    existing = list(INPUT_DIR.glob("*.csv"))
    if existing:
        print(f"File already exists: {existing[0].name}. Skipping.")
        return

    print(f"Downloading dataset: {DATASET}")
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(DATASET, path=str(INPUT_DIR), unzip=True)

    downloaded = list(INPUT_DIR.glob("*.csv"))
    if downloaded:
        for f in downloaded:
            print(f"Downloaded {f.stat().st_size / (1024*1024):.1f} MB -> {f}")
    else:
        print("ERROR: No CSV files found after download.")


if __name__ == "__main__":
    main()
