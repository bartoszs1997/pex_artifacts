# Sales Data Catalog

This document defines how sales data is organized in the data lake so that any
file can be located quickly and its contents understood without opening it. It
covers the naming convention, the folder hierarchy, the metadata attributes, and
how to navigate and search the catalog.

The data lake is imitated with LocalStack S3. All paths below live in the bucket
`s3://pex-datalake/`.

## Source data

The catalog organizes the Video Game Sales dataset
(`gregorut/videogamesales`, `vgsales.csv`), treated as sales records. Its columns
are `Rank, Name, Platform, Year, Genre, Publisher, NA_Sales, EU_Sales, JP_Sales,
Other_Sales, Global_Sales`, where `*_Sales` are units sold in millions.

Domain mapping used throughout the catalog:

| Catalog concept | Dataset column | Meaning |
|---|---|---|
| Source system  | (constant `vgsales`) | Origin of the records |
| Business unit  | `Genre` | The product line the sales belong to (Action, Sports, ...) |
| Date           | `Year` | Sale year; the dataset is annual (no month/quarter) |

## Naming convention

Each organized data file is named:

```
sales_data_{source}_{business_unit}_{year}.csv
```

- `source` - the source system, here always `vgsales`.
- `business_unit` - a lowercase, underscore-safe genre slug (e.g. `role_playing`).
- `year` - the four-digit sale year.

Example: `sales_data_vgsales_action_2016.csv` holds Action sales for 2016.

The convention embeds the source, business unit, and date directly in the file
name, so a file is self-describing even outside the catalog.

## Folder hierarchy

Files are grouped by year, one folder per year:

```
sales_catalog/
  Sales_1980/
    sales_data_vgsales_action_1980.csv
    sales_data_vgsales_action_1980.metadata.json
    ...
  Sales_1981/
  ...
  Sales_2020/
```

- `sales_catalog/` - the organized (curated) zone of the lake.
- `Sales_{year}/` - one folder per year (the primary partition). The acceptance
  criterion asks for organization "based on year or quarter"; the dataset is
  annual, so we partition by year. The metadata schema also carries a `quarter`
  field (set to `FY`, full year) so the hierarchy can extend to quarters if a
  finer-grained source is added later.
- Within a year folder there is one data file per business unit, plus a matching
  `*.metadata.json` sidecar.

## Metadata attributes

Every data file has metadata stored in two places:

1. A per-file sidecar `*.metadata.json` next to the data file.
2. A row in the central catalog index (a Delta table at
   `s3://pex-datalake/catalog_index`), which is what searches query.

Both hold the same attributes:

| Attribute | Example | Description |
|---|---|---|
| `file_path` | `s3a://pex-datalake/sales_catalog/Sales_2016/sales_data_vgsales_action_2016.csv` | Full lake path to the data file |
| `file_name` | `sales_data_vgsales_action_2016.csv` | File name (follows the convention) |
| `data_source` | `vgsales` | Source system the records came from |
| `business_unit` | `Action` | Product line (genre) |
| `year` | `2016` | Sale year |
| `quarter` | `FY` | Time granularity (full year for this source) |
| `file_format` | `csv` | Physical format of the data file |
| `row_count` | `225` | Number of sales records in the file |
| `description` | `Video game sales records for Action titles in 2016.` | Human-readable summary |
| `owner` | `sales-analytics-team` | Team accountable for the data |
| `tags` | `[vgsales, sales, action, 2016]` | Free-form labels for discovery |
| `created_at` | `2026-07-13T12:00:00+00:00` | When the catalog entry was created |

## How to navigate

- To browse by time, open the `Sales_{year}/` folder for the year you want.
- To read one file's metadata, open its `*.metadata.json` sidecar.
- To search across everything, query the catalog index (below) instead of
  listing folders.

## How to search

The catalog index is a Delta table registered as the SQL view `catalog`. Typical
searches (run by `catalog.py`):

- By date range:
  `SELECT * FROM catalog WHERE year BETWEEN 2014 AND 2016`
- By business unit:
  `SELECT * FROM catalog WHERE business_unit = 'Role-Playing'`
- By file format:
  `SELECT * FROM catalog WHERE file_format = 'csv'`

Each query returns the matching `file_path` values, so a search result points
straight at the files to open.
