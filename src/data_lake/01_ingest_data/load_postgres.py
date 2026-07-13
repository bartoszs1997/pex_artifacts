"""Task 01 - load housing data into PostgreSQL (the streaming SOURCE).

The streaming script reads rows from Postgres. To have rows to read, this script
creates a table and inserts data from housing.csv into it.

    --rows N   insert the first N rows and exit
    --stream   after that, keep inserting one row per second, so you can watch
               stream_ingest.py capture the new rows live

Prerequisite: the database is up (docker compose ... up -d).

Usage:
    uv run python src/data_lake/01_ingest_data/load_postgres.py --rows 200
    uv run python src/data_lake/01_ingest_data/load_postgres.py --rows 200 --stream
"""

import argparse
import csv
import time
from pathlib import Path

import psycopg2

CSV_FILE = Path(__file__).resolve().parent / "data" / "input" / "housing.csv"
DSN = "host=localhost port=5432 dbname=pexsource user=pex password=pex"

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS housing (
    id            BIGINT PRIMARY KEY,
    med_inc       DOUBLE PRECISION,
    house_age     DOUBLE PRECISION,
    ave_rooms     DOUBLE PRECISION,
    ave_bedrms    DOUBLE PRECISION,
    population    DOUBLE PRECISION,
    ave_occup     DOUBLE PRECISION,
    latitude      DOUBLE PRECISION,
    longitude     DOUBLE PRECISION,
    med_house_val DOUBLE PRECISION
);
"""

INSERT_SQL = """
INSERT INTO housing VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING;
"""


def read_rows() -> list[tuple]:
    """Read housing.csv into tuples. The CSV's first column is used as the id."""
    rows = []
    with CSV_FILE.open(newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip the header
        for raw in reader:
            rows.append((int(raw[0]), *[float(x) for x in raw[1:]]))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200, help="rows to insert first")
    parser.add_argument("--stream", action="store_true", help="keep inserting slowly")
    args = parser.parse_args()

    rows = read_rows()
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(CREATE_SQL)
    cur.executemany(INSERT_SQL, rows[: args.rows])
    print(f"Inserted {args.rows} rows into housing")

    if args.stream:
        print("Streaming inserts (1/sec). Ctrl+C to stop.")
        try:
            for row in rows[args.rows :]:
                cur.execute(INSERT_SQL, row)
                print(f"Inserted id={row[0]}")
                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopped.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
