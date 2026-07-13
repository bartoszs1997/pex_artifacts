# Task 07 - Organize data in a data catalog

Organize a flat sales file that sits in the data lake into a discoverable **data
catalog**: split it into files that follow a naming convention, lay them out in a
year-based folder hierarchy, attach metadata to every file, and populate a
searchable catalog index that can locate files by date range, business unit, or
file format.

## Dataset

[Video Game Sales](https://www.kaggle.com/datasets/gregorut/videogamesales)
(`gregorut/videogamesales`, `vgsales.csv`, ~16.6k rows). Treated as sales data:
`Genre` is the business unit, `Year` is the sale date, `*_Sales` are units sold
(millions). Downloaded into `data/input/` (gitignored).

Download:

```bash
uv run python src/data_lake/07_data_catalog/download_data.py
```

## What it builds

The data lake is imitated with **LocalStack S3** (bucket `pex-datalake`).

1. **Organized files** under `sales_catalog/Sales_{year}/`, one CSV per
   `(year, business unit)`, named `sales_data_vgsales_{business_unit}_{year}.csv`.
2. **Per-file metadata** as a `*.metadata.json` sidecar next to each data file.
3. **Catalog index** at `catalog_index` (a Delta table, one row per data file),
   registered as the SQL view `catalog` and used for searches.

The full catalog design - naming convention, folder hierarchy, metadata
attributes, navigation and search instructions - is documented in
[`CATALOG.md`](./CATALOG.md).

## How to run

Start the lake, then run the catalog program (Java 17 must be on PATH for
PySpark):

```bash
docker compose -f src/data_lake/07_data_catalog/docker-compose.yml up -d

export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/data_lake/07_data_catalog/catalog.py

docker compose -f src/data_lake/07_data_catalog/docker-compose.yml down -v
```

## How to verify

`catalog.py` runs three search queries (by date range, by business unit, by file
format) and prints their results, then a `verify()` step asserts:

- every organized data file has exactly one catalog entry (files == entries);
- required metadata (`data_source`, `description`, `owner`, `business_unit`,
  `year`) is present on every entry;
- every file name matches the naming-convention regex;
- a date-range search returns results (the catalog is discoverable).

Expected final line:

```
VERIFIED: catalog holds 389 files across years 1980-2020 and 12 business units;
every file follows the naming convention and carries complete metadata; searches
return results
```

## Acceptance criteria mapping

| Criterion | Where it is met |
|---|---|
| Files named per the naming convention | `organize()` builds `sales_data_{source}_{unit}_{year}.csv`; `verify()` regex-checks every name |
| Files organized in a folder hierarchy by year/quarter | `Sales_{year}/` folders in `sales_catalog/`; metadata carries `quarter` |
| Metadata attributes (source, description, owner) per file | `organize()` writes a metadata entry + JSON sidecar per file |
| Catalog populated with organized files + complete metadata | `build_index()` writes the Delta catalog index; `verify()` checks completeness |
| Search/query tests demonstrate discoverability | `run_searches()` queries by date range, business unit, file format |
| Documentation of catalog structure and conventions | [`CATALOG.md`](./CATALOG.md) |

## Dependencies

`pyspark==4.1.1`, `delta-spark==4.3.0`, `boto3`, `pandas` (all in the root
`pyproject.toml`). LocalStack S3 via Docker. Spark pulls `delta-spark` and
`hadoop-aws` jars on first run (cached in `~/.ivy2`).
