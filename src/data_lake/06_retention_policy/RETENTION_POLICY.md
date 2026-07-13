# Data Retention Policy Framework - Financial Data Lake

This document is the retention policy framework required by the task. It defines,
for each type of data stored in the data lake, how long it is kept, why, and what
happens when the retention period expires. The `retention.py` script enforces
exactly the rules stated here.

## 1. Legal requirements

The data lake stores US fund and market-price data, so it falls under financial
recordkeeping regulation. The framework is aligned with:

- **SEC Rule 17a-4** - broker-dealers must preserve certain records (including
  transaction and market/price records) for **6-7 years**, the first two years in
  an easily accessible place.
- **FINRA Rule 4511** - books and records must be kept for at least **6 years**
  unless another rule specifies otherwise.
- **Sarbanes-Oxley (SOX) Section 802** - audit and financial records retained for
  **7 years**.

We therefore adopt **7 years** as the retention period for price/market records,
which satisfies the strictest of the above.

## 2. Business requirements

- Analysts need several years of price history for back-testing and trend
  analysis; less than ~5 years reduces analytical value.
- Storage cost grows with unbounded history, so data past its legal obligation is
  moved to a cheaper archive tier and then removed.
- Reference/metadata (fund descriptions) must stay queryable for as long as the
  fund is tracked, plus a grace period after it is delisted.

## 3. Data types, sensitivity and retention periods

| Data set | Data type | Sensitivity | Retention | Legal basis |
|---|---|---|---|---|
| `etf_prices` | Time-series market/price records | Low (public market data) | **7 years** from `price_date` | SEC 17a-4 / SOX 802 |
| `etf_metadata` | Reference/master data | Low (public) | **Retained while active** (no time-based expiry) | Business need |

Sensitivity note: this dataset is public market data, so there is no PII. If PII
were present (e.g. customer accounts), a shorter, privacy-driven retention
(GDPR/CCPA "storage limitation") would override and take precedence.

## 4. Lifecycle stages and actions

For time-based data (`etf_prices`), each record moves through these stages based
on its `price_date`:

1. **Active** - `price_date` within the last 7 years. Kept in the active Delta
   table, fully queryable.
2. **Expired -> Archived** - `price_date` older than 7 years. The record is
   copied to the archive Delta table (the cheaper archive tier) and then removed
   from the active table.
3. **Physically removed** - after the records are deleted from the active table,
   Delta `VACUUM` removes the underlying data files so storage is actually
   reclaimed (not just logically hidden).

Reference data (`etf_metadata`) has no time-based expiry, so it is tagged and
retained; the engine reports it as "retain while active" and does not delete it.

## 5. Archiving system

The archive tier is a separate Delta table (`etf_prices_archive`) under an
`archive/` prefix in the data lake. In a real cloud deployment this prefix would
map to a colder, cheaper storage class (S3 Glacier / Azure Cool / GCS Coldline)
via a storage lifecycle rule. Archived data remains restorable but is not part of
the hot, active dataset.

## 6. Enforcement and metadata tagging

Policies are declared as metadata in `retention.py` (`RETENTION_POLICIES`) and
attached to each table (`date_column`, `retention_years`, `action`, `legal_basis`,
`sensitivity`). The retention engine reads this metadata and applies the rules
uniformly, so the policy is defined in one place and enforced consistently -
guaranteeing compliance, data security, and optimized storage utilization.

## 7. Reference "now"

For a reproducible demonstration the engine uses a fixed policy reference date,
`POLICY_NOW = 2021-12-31` (the dataset's "as of" date), instead of the wall-clock
date. With a 7-year retention this yields a cutoff of `2014-12-31`: price records
on or before that date are archived and removed, later records are retained.
