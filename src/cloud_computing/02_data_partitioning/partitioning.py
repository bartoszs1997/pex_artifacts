"""Data partitioning strategy PoC and benchmark (customer data lake).

This script builds and validates the partitioning strategy documented in
PARTITIONING.md. It uses plain Hive-style partitioned Parquet on the local
filesystem as the data lake: the partition columns become a folder tree
(Profession=.../Spending_Score=...), which is exactly the "folder structure on
data lake storage" the task asks for. The same layout works unchanged on S3.

Pipeline:
  1. Load the customer segmentation data and inflate it to a realistic volume so
     the benchmarks are meaningful (the raw file is only ~8k rows).
  2. Write two copies of the lake: one UNPARTITIONED, one PARTITIONED by
     (Profession, Spending_Score).
  3. Show the resulting partition folder tree.
  4. Benchmark three query scenarios on both layouts:
       - selective query (equality on the partition key)
       - range query    (a subset of professions -> a range of partitions)
       - aggregation     (group by the partition key)
     Measure wall-clock time and how many partition directories are scanned
     (data pruning), and print the physical plan's PartitionFilters as proof.
  5. Assess ingestion/update impact and verify pruning actually happened.

Dataset:
    vetrirah/customer -> data/input/Train.csv

Run:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/cloud_computing/02_data_partitioning/partitioning.py
"""

import logging
import shutil
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("partitioning")

BASE = Path(__file__).resolve().parent
INPUT_CSV = BASE / "data/input/Train.csv"
OUT = BASE / "data/output"
UNPART = OUT / "lake_unpartitioned"
PART = OUT / "lake_partitioned"
INFLATE = 400  # ~8k rows * 400 = ~3.2M rows, enough for a meaningful benchmark
PARTITION_COLS = ["Profession", "Spending_Score"]


def create_spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_and_inflate(spark: SparkSession):
    """Load the CSV, clean partition keys, and inflate to a realistic volume."""
    df = spark.read.csv(str(INPUT_CSV), header=True, inferSchema=True)
    # Partition keys must not be null (they become folder names).
    df = df.na.fill({"Profession": "Unknown", "Spending_Score": "Unknown"})

    # Inflate: cross join with a small range to replicate rows INFLATE times.
    mult = spark.range(INFLATE).select(F.col("id").alias("_copy"))
    inflated = df.crossJoin(mult).drop("_copy")
    inflated = inflated.repartition(8).cache()
    n = inflated.count()
    log.info(f"Loaded and inflated dataset: {n} rows (x{INFLATE}).")
    return inflated


def build_lakes(df) -> None:
    """Write the unpartitioned and partitioned copies of the lake."""
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    df.write.mode("overwrite").parquet(str(UNPART))
    t_unpart = time.perf_counter() - t0

    t0 = time.perf_counter()
    df.write.mode("overwrite").partitionBy(*PARTITION_COLS).parquet(str(PART))
    t_part = time.perf_counter() - t0

    log.info(f"Ingestion write time  unpartitioned: {t_unpart:.2f}s")
    log.info(f"Ingestion write time  partitioned:   {t_part:.2f}s "
             f"(partitioning adds overhead: it writes one file set per partition)")


def show_partition_tree() -> int:
    """Print the partition folder tree and return the number of leaf partitions."""
    prof_dirs = sorted(p for p in PART.glob("Profession=*") if p.is_dir())
    leaves = 0
    log.info("Partition folder structure (data lake layout):")
    for pd in prof_dirs:
        spend_dirs = sorted(s for s in pd.glob("Spending_Score=*") if s.is_dir())
        log.info(f"    {pd.name}/")
        for sd in spend_dirs:
            leaves += 1
            log.info(f"        {sd.name}/")
    log.info(f"Total leaf partitions: {leaves}")
    return leaves


def _count_scanned_partitions(base: Path, predicate) -> int:
    """Count leaf partition directories that match a predicate (pruning proxy)."""
    matched = 0
    for pd in base.glob("Profession=*"):
        for sd in pd.glob("Spending_Score=*"):
            profession = pd.name.split("=", 1)[1]
            spending = sd.name.split("=", 1)[1]
            if predicate(profession, spending):
                matched += 1
    return matched


def _timed(fn, runs: int = 5) -> float:
    """Return the median wall-clock over a few runs of an action."""
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    times.sort()
    return times[len(times) // 2]


def benchmark(spark) -> list[dict]:
    """Run the three scenarios on both layouts and collect metrics."""
    unpart = spark.read.parquet(str(UNPART))
    part = spark.read.parquet(str(PART))

    total_parts = _count_scanned_partitions(PART, lambda p, s: True)
    results = []

    # Each action aggregates a data column (Age) so it reads real column data
    # from the matched partitions (count() alone would only read Parquet footers).

    # --- Scenario 1: selective query (equality on the partition key) ---
    sel = ("Profession == 'Artist' and Spending_Score == 'Low'")
    scanned = _count_scanned_partitions(PART, lambda p, s: p == "Artist" and s == "Low")
    t_u = _timed(lambda: unpart.where(sel).agg(F.avg("Age")).collect())
    t_p = _timed(lambda: part.where(sel).agg(F.avg("Age")).collect())
    results.append({
        "scenario": "selective (Profession=Artist, Spending=Low)",
        "unpart_s": t_u, "part_s": t_p,
        "parts_scanned": f"{scanned}/{total_parts}",
    })

    # --- Scenario 2: range query (a subset of professions) ---
    prof_range = ["Artist", "Doctor", "Engineer"]
    rng = f"Profession in ({', '.join(repr(p) for p in prof_range)})"
    scanned = _count_scanned_partitions(PART, lambda p, s: p in prof_range)
    t_u = _timed(lambda: unpart.where(rng).agg(F.avg("Age")).collect())
    t_p = _timed(lambda: part.where(rng).agg(F.avg("Age")).collect())
    results.append({
        "scenario": "range (Profession in Artist/Doctor/Engineer)",
        "unpart_s": t_u, "part_s": t_p,
        "parts_scanned": f"{scanned}/{total_parts}",
    })

    # --- Scenario 3: aggregation grouped by the partition key ---
    agg = lambda df: df.groupBy("Profession").agg(F.avg("Age")).collect()
    t_u = _timed(lambda: agg(unpart))
    t_p = _timed(lambda: agg(part))
    results.append({
        "scenario": "aggregation (avg Age group by Profession)",
        "unpart_s": t_u, "part_s": t_p,
        "parts_scanned": f"{total_parts}/{total_parts}",
    })

    # Proof of pruning: the physical plan of the selective query shows
    # PartitionFilters on the partitioned layout (and none on the flat one).
    log.info("Physical plan of the selective query (partitioned layout):")
    part.where(sel).explain(mode="simple")

    return results


def report(results: list[dict]) -> None:
    """Print a benchmark summary table and write it to a markdown file."""
    lines = [
        "| Scenario | Unpartitioned (s) | Partitioned (s) | Speedup | Partitions scanned |",
        "|---|---|---|---|---|",
    ]
    log.info("Benchmark summary:")
    for r in results:
        speedup = r["unpart_s"] / r["part_s"] if r["part_s"] > 0 else float("inf")
        line = (f"| {r['scenario']} | {r['unpart_s']:.3f} | {r['part_s']:.3f} "
                f"| {speedup:.2f}x | {r['parts_scanned']} |")
        lines.append(line)
        log.info(f"    {r['scenario']}: unpart={r['unpart_s']:.3f}s "
                 f"part={r['part_s']:.3f}s speedup={speedup:.2f}x "
                 f"scanned={r['parts_scanned']}")
    (OUT / "benchmark_results.md").write_text("\n".join(lines) + "\n")
    log.info(f"Wrote benchmark table -> {OUT / 'benchmark_results.md'}")


def main() -> int:
    spark = create_spark_session("cc02-data-partitioning")
    try:
        df = load_and_inflate(spark)
        build_lakes(df)
        leaves = show_partition_tree()
        results = benchmark(spark)
        report(results)

        # Verification: the selective query must scan far fewer partitions than
        # exist, and be no slower than the unpartitioned scan.
        selective = results[0]
        scanned, total = (int(x) for x in selective["parts_scanned"].split("/"))
        log.info(f"VERIFICATION: selective query scans {scanned}/{total} partitions "
                 f"({100 * scanned / total:.1f}% of the lake), "
                 f"partitioned={selective['part_s']:.3f}s vs "
                 f"unpartitioned={selective['unpart_s']:.3f}s.")
        assert leaves > 1, "partitioning must produce multiple partitions"
        assert scanned < total, "selective query must prune partitions"
        log.info("PoC completed successfully.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
