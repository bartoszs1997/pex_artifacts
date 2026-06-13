"""Create and populate the MySQL `series_ratings` table.

This is the *load* step for the optional JDBC part of the task. It connects to
the local MySQL server with the native Python connector (mysql.connector) and
runs `sql/setup_ratings.sql`, which:
    * creates the `pex` database (if missing),
    * (re)creates the `series_ratings` table,
    * inserts a handful of rows whose series_id joins to the JSON series id.

The script is idempotent (CREATE DATABASE IF NOT EXISTS / DROP TABLE IF EXISTS),
so it can be re-run safely. The companion script
`optional_data_read_and_transform.py` then reads this table over JDBC with Spark.

Credentials are read from a gitignored `.env` file at the repo root
(MYSQL_USER / MYSQL_PASSWORD); copy `.env.example` to `.env` and fill it in.

Run (from the repo root, after the MySQL server is started):
    uv run python src/batch_processing/tv_series/optional_data_load.py
"""

import logging
import os
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv
from mysql.connector.cursor import MySQLCursor
from mysql.connector.pooling import PooledMySQLConnection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("mysql_loader")

SQL_DIR = Path(__file__).resolve().parent / "sql"
SETUP_SCRIPT = SQL_DIR / "setup_ratings.sql"


def read_sql_script(path: Path) -> str:
    """Read a .sql file into a string."""
    log.info(f"Reading SQL script from {path}...")
    return path.read_text(encoding="utf-8")


def split_statements(script: str) -> list[str]:
    """Split a SQL script into individual executable statements.

    mysql.connector's cursor runs one statement per execute() call, so we strip
    comments (anything after `--` on a line) and split the script on `;`, then
    drop empty fragments. This keeps the schema and seed data in the .sql file
    rather than hard-coded in Python.
    """
    lines = []
    for line in script.splitlines():
        if "--" in line:
            line = line[: line.index("--")]
        lines.append(line)
    cleaned = "\n".join(lines)
    return [statement.strip() for statement in cleaned.split(";") if statement.strip()]


def create_connection() -> PooledMySQLConnection:
    """Connect to the local MySQL server.

    No database is selected on purpose: the SQL script runs CREATE DATABASE /
    USE itself, so the loader can bootstrap a fresh server.
    """
    log.info("Connecting to MySQL...")
    return mysql.connector.connect(
        host="localhost",
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
    )


def run_setup(cursor: MySQLCursor, statements: list[str]) -> None:
    """Execute each statement of the setup script in order."""
    for statement in statements:
        preview = statement.splitlines()[0][:60]
        log.info(f"Executing: {preview} ...")
        cursor.execute(statement)


def main() -> None:
    """Run the setup script against MySQL."""
    load_dotenv()  # read MYSQL_USER / MYSQL_PASSWORD from the repo-root .env
    statements = split_statements(read_sql_script(SETUP_SCRIPT))

    connection: PooledMySQLConnection = create_connection()
    cursor: MySQLCursor = connection.cursor()
    try:
        run_setup(cursor, statements)
        connection.commit()
        log.info(f"Done: executed {len(statements)} statements against MySQL.")
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()