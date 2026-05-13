"""
Exploration script to sanity-check the UpCloud billing bucket before writing
the real pipeline.

The bucket lives at:
  https://u1pfm.upcloudobjects.com/dwh-fina/billing/year=YYYY/month=MM/day=DD/billing.csv

It allows public HTTPS reads of specific files but rejects anonymous S3 LIST,
so we can't use a glob like year=*/month=*/day=*/billing.csv. Instead we
generate the list of partition URLs in Python and pass them to DuckDB
explicitly. The real pipeline will do the same thing, just driven by state
instead of a hardcoded date range.

What this script verifies:
  1. DuckDB can read one file from the bucket
  2. DuckDB can read several files in one query
  3. The schema looks like what the assignment described
  4. Basic aggregations work
  5. hive_partitioning=true overrides typed year/month/day with strings,
     so we keep it off and trust the CSV's own columns
"""

from datetime import date, timedelta
import duckdb

BUCKET_BASE = "https://u1pfm.upcloudobjects.com/dwh-fina/billing"


def partition_url(d: date) -> str:
    """Build the public URL for one day's CSV."""
    return f"{BUCKET_BASE}/year={d.year}/month={d.month:02d}/day={d.day:02d}/billing.csv"


def date_range(start: date, end: date):
    """Yield every date from start to end, inclusive on both ends."""
    days = (end - start).days
    for i in range(days + 1):
        yield start + timedelta(days=i)


con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Hardcoded one-week window for exploration. The real pipeline computes
# this range from state instead.
urls = [partition_url(d) for d in date_range(date(2024, 1, 1), date(2024, 1, 7))]
print(f"Reading {len(urls)} partitions: {urls[0]} ... {urls[-1]}")


# Check 1: one specific file
print("\n=== Check 1: single file ===")
single = con.execute(f"""
    SELECT COUNT(*) AS row_count
    FROM read_csv('{urls[0]}')
""").fetchone()
print(f"Rows in {urls[0]}: {single[0]}")


# Check 2: pass the full list to one read_csv call
print("\n=== Check 2: list of files ===")
total_count = con.execute("""
    SELECT COUNT(*) AS row_count
    FROM read_csv(?, hive_partitioning = false)
""", [urls]).fetchone()
print(f"Total rows across {len(urls)} partitions: {total_count[0]}")


# Check 3: schema and one sample row
print("\n=== Check 3: schema and sample ===")
sample = con.execute("""
    SELECT *
    FROM read_csv(?, hive_partitioning = false)
    LIMIT 1
""", [urls]).fetchdf()
print(sample.T)


# Check 4: a basic aggregation
print("\n=== Check 4: credit usage by region ===")
agg = con.execute("""
    SELECT
        region,
        COUNT(*) AS row_count,
        ROUND(SUM(credit_usage), 2) AS total_credit_usage,
        ROUND(AVG(credit_usage), 4) AS avg_credit_usage
    FROM read_csv(?, hive_partitioning = false)
    GROUP BY region
    ORDER BY total_credit_usage
""", [urls]).fetchdf()
print(agg)


# Check 5: compare schemas with and without hive_partitioning. When the CSV
# already has year/month/day, hive_partitioning=true silently replaces them
# with VARCHARs parsed from the folder path. We keep it off.
print("\n=== Check 5: hive_partitioning effect on schema ===")

cols_with = con.execute(
    "DESCRIBE SELECT * FROM read_csv(?, hive_partitioning = true)",
    [urls[:1]]
).fetchdf()
print("With hive_partitioning=true:")
print(cols_with[["column_name", "column_type"]])

cols_without = con.execute(
    "DESCRIBE SELECT * FROM read_csv(?, hive_partitioning = false)",
    [urls[:1]]
).fetchdf()
print("\nWith hive_partitioning=false:")
print(cols_without[["column_name", "column_type"]])