"""Download the movie ratings dataset.

Dataset:
    MovieLens Latest (small) — GroupLens Research
    https://grouplens.org/datasets/movielens/

    The original task references "Cinemagoer" but that Kaggle dataset no longer
    exists (404) and the cinemagoer scraping library is broken (IMDb changed
    their HTML). MovieLens is the standard academic movie-ratings dataset with
    the same structure: multiple CSV files containing user ratings and genres.

Output:
    Downloaded and unzipped into ./data/input next to this script.
    That folder is gitignored (see .gitignore: **/data/input/).

    Files:
      - movies.csv   (movieId, title, genres)
      - ratings.csv  (userId, movieId, rating, timestamp)
      - tags.csv     (userId, movieId, tag, timestamp)
      - links.csv    (movieId, imdbId, tmdbId)

Usage:
    uv run python download_data.py
    (run from src/programming_languages/01_cinemagoer directory)
"""

import logging
import shutil
import ssl
import urllib.request
import zipfile
from pathlib import Path

import certifi

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("download")

URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
DATA_DIR = Path(__file__).resolve().parent / "data" / "input"


def download() -> None:
    """Fetch MovieLens zip over HTTPS, unzip into DATA_DIR."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = DATA_DIR / "ml-latest-small.zip"

    log.info(f"Downloading {URL}")
    context = ssl.create_default_context(cafile=certifi.where())
    request = urllib.request.Request(URL, headers={"User-Agent": "pex-artifacts/1.0"})
    with urllib.request.urlopen(request, context=context) as resp, zip_path.open("wb") as out:
        shutil.copyfileobj(resp, out)
    log.info(f"Saved zip ({zip_path.stat().st_size / 1_000_000:.1f} MB)")

    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            if member.endswith(".csv"):
                # Flatten: strip the top-level directory from the zip path
                filename = Path(member).name
                target = DATA_DIR / filename
                with zf.open(member) as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                log.info(f"  {filename} ({target.stat().st_size / 1024:.0f} KB)")

    zip_path.unlink()
    log.info("Done.")


if __name__ == "__main__":
    download()