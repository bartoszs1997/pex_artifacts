# Task 06 - Set up retention policy rules (Delta Lake)

Establish a **retention policy framework** for a financial data lake, configure
the lake to enforce it, and verify that data is retained for its required
duration and archived/removed when it expires. The data lake is imitated with
**LocalStack S3**.

The policy framework itself (legal, business, data-type, sensitivity, archiving)
is documented in **[`RETENTION_POLICY.md`](./RETENTION_POLICY.md)**; `retention.py`
implements exactly those rules.

```
active  : s3a://pex-datalake/active/etf_prices     (Delta, time-series, partitioned by year)
          s3a://pex-datalake/active/etf_metadata   (Delta, reference data)
archive : s3a://pex-datalake/archive/etf_prices    (Delta, archive tier)
```

## Dataset

US Funds dataset (`stefanoleone992/mutual-funds-and-etfs`). We use:

- `ETF prices.csv` (3.8M rows) - time-series `fund_symbol, price_date, open,
  high, low, close, adj_close, volume`. Sampled to the first 100,000 rows
  (dates 2002-2021) to keep local Spark fast.
- `ETFs.csv` (2,310 rows) - fund reference/metadata.

## Retention policy (summary)

| Table | Retention | Action on expiry | Legal basis |
|---|---|---|---|
| `etf_prices` | 7 years from `price_date` | archive, then delete, then VACUUM | SEC 17a-4 / FINRA 4511 / SOX 802 |
| `etf_metadata` | retain while active | none (no time-based expiry) | business need |

Reference "now" is fixed at `POLICY_NOW = 2021-12-31` (the dataset's as-of date)
so the demo is reproducible; the 7-year cutoff is therefore `2014-12-31`.

## How the enforcement works

1. **Load / configure** - fund data is written into active Delta tables; the
   price table is partitioned by year for lifecycle management.
2. **Metadata tagging** - `RETENTION_POLICIES` in `retention.py` declares each
   table's `date_column`, `retention_years`, `action`, `sensitivity` and
   `legal_basis`. The engine reads this metadata and applies it uniformly.
3. **Archive** - price rows with `price_date <= cutoff` are written to the
   archive-tier Delta table.
4. **Delete** - the same expired rows are deleted from the active table
   (Delta `DELETE`).
5. **Reclaim storage** - Delta `VACUUM` physically removes the now-unreferenced
   data files (run with a zero-hour retention for the demo, enabled via
   `spark.databricks.delta.retentionDurationCheck.enabled=false`).

In a real cloud deployment the `archive/` prefix would map to a colder storage
class (S3 Glacier / Azure Cool / GCS Coldline) through a storage lifecycle rule;
here the separate archive Delta table plays that role.

## How to run

```bash
# 1. Download the dataset (Kaggle credentials read from ~/.kaggle/)
uv run python src/data_lake/06_retention_policy/download_data.py

# 2. Bring up the data lake (LocalStack S3)
docker compose -f src/data_lake/06_retention_policy/docker-compose.yml up -d

# 3. Run the retention engine (Java 17 must be on PATH for PySpark)
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/data_lake/06_retention_policy/retention.py

# 4. Tear the data lake down
docker compose -f src/data_lake/06_retention_policy/docker-compose.yml down -v
```

## How to verify

The final line prints, for example:

```
VERIFIED: active retains 71870 rows (oldest 2015-01-02, all after cutoff
2014-12-31); archive holds 28130 expired rows; etf_metadata retained while active
```

`verify()` asserts:
- the active table has **no** record on or before the cutoff (retention window
  enforced),
- the archive holds **exactly** the expired rows (nothing lost, proper
  archiving),
- every archived row is genuinely older than the cutoff.

## Dependencies and configuration

- **PySpark 4.1.1** (host), **Java 17** on PATH.
- Spark packages pulled from Maven on first run (cached in `~/.ivy2`):
  - `io.delta:delta-spark_2.13:4.3.0` - Delta Lake (`DELETE`, `VACUUM`).
  - `org.apache.hadoop:hadoop-aws:3.4.2` - the `s3a://` filesystem.
- **boto3** to create the bucket and reset the lake between demo runs.
- **LocalStack S3 3.8** (pinned; `latest` is a Pro build that fails activation).

## Acceptance criteria mapping

- *Retention policy framework with well-defined guidelines* -> `RETENTION_POLICY.md`
  (legal, business, data types, sensitivity, lifecycle, archiving).
- *Data lake configured in alignment with the policy (lifecycle actions,
  archival/deletion)* -> `retention.py` loads the data, tags it with policy
  metadata, and runs archive -> delete -> VACUUM.
- *Policies operational and effectively enforced (compliance, security, optimized
  storage)* -> the verification step proves the active table is within policy and
  the expired data is archived; VACUUM reclaims storage.
