"""Salary data analysis — BEFORE refactoring.

This is the original, unrefactored version of the salary analysis script.
It works correctly but has multiple code quality issues documented in README.md.
The refactored version is in solution.py.

Dataset: Employee Salaries (ds_salaries.csv, 607 rows, 12 columns)

Usage:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/programming_languages/11_refactoring/solution_before.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.appName("salary").master("local[*]").getOrCreate()

# load data
path = os.path.dirname(os.path.abspath(__file__)) + "/data/input/ds_salaries.csv"
df = spark.read.csv(path, header=True, inferSchema=True)
print("loaded " + str(df.count()) + " rows")

# analysis 1 - avg salary by experience
result1 = df.groupBy("experience_level").agg(F.avg("salary_in_usd").alias("avg_salary"), F.count("*").alias("count")).orderBy(F.desc("avg_salary"))
result1.show()
result1.coalesce(1).write.mode("overwrite").csv(os.path.dirname(os.path.abspath(__file__)) + "/data/output/by_experience", header=True)

# analysis 2 - avg salary by company size
result2 = df.groupBy("company_size").agg(F.avg("salary_in_usd").alias("avg_salary"), F.count("*").alias("count")).orderBy(F.desc("avg_salary"))
result2.show()
result2.coalesce(1).write.mode("overwrite").csv(os.path.dirname(os.path.abspath(__file__)) + "/data/output/by_company_size", header=True)

# analysis 3 - avg salary by remote ratio
result3 = df.groupBy("remote_ratio").agg(F.avg("salary_in_usd").alias("avg_salary"), F.count("*").alias("count")).orderBy(F.desc("avg_salary"))
result3.show()
result3.coalesce(1).write.mode("overwrite").csv(os.path.dirname(os.path.abspath(__file__)) + "/data/output/by_remote_ratio", header=True)

# analysis 4 - top 10 job titles
all_jobs = df.collect()
job_salaries = {}
for row in all_jobs:
    title = row["job_title"]
    salary = row["salary_in_usd"]
    if title in job_salaries:
        job_salaries[title].append(salary)
    else:
        job_salaries[title] = [salary]
top_jobs = []
for title, salaries in job_salaries.items():
    avg = sum(salaries) / len(salaries)
    top_jobs.append((title, round(avg, 2), len(salaries)))
top_jobs.sort(key=lambda x: x[1], reverse=True)
top_10 = top_jobs[:10]
for job in top_10:
    print(f"{job[0]}: ${job[1]:,.2f} (n={job[2]})")

# analysis 5 - salary tiers
low = df.filter(F.col("salary_in_usd") < 50000)
mid = df.filter((F.col("salary_in_usd") >= 50000) & (F.col("salary_in_usd") < 100000))
high = df.filter(F.col("salary_in_usd") >= 100000)
print("Low salary: " + str(low.count()))
print("Mid salary: " + str(mid.count()))
print("High salary: " + str(high.count()))
low.coalesce(1).write.mode("overwrite").csv(os.path.dirname(os.path.abspath(__file__)) + "/data/output/tier_low", header=True)
mid.coalesce(1).write.mode("overwrite").csv(os.path.dirname(os.path.abspath(__file__)) + "/data/output/tier_mid", header=True)
high.coalesce(1).write.mode("overwrite").csv(os.path.dirname(os.path.abspath(__file__)) + "/data/output/tier_high", header=True)

# analysis 6 - year over year
years = [r["work_year"] for r in df.select("work_year").distinct().collect()]
years.sort()
for y in years:
    year_df = df.filter(F.col("work_year") == y)
    avg = year_df.agg(F.avg("salary_in_usd")).collect()[0][0]
    cnt = year_df.count()
    print(f"Year {y}: avg=${avg:,.2f}, count={cnt}")

spark.stop()
