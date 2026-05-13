"""
UpCloud billing pipeline.

Reads daily Hive-partitioned CSVs from UpCloud Object Storage, loads them
into a local DuckDB warehouse, then runs dbt to produce marts.

Idempotent: re-running the script reloads partitions in place rather than
appending. State is tracked in the processed_partitions table inside the
DuckDB file.
"""

import logging
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

# --- Configuration ----------------------------------------------------------

BUCKET_BASE = "https://u1pfm.upcloudobjects.com/dwh-fina/billing"
START_DATE = date(2024, 1, 1)
# END_DATE = date(2024, 1, 3)  # TEMP: smoke test
DB_PATH = Path("warehouse.duckdb")
DBT_PROJECT_DIR = Path("dbt_project")
LOG_DIR = Path("logs")

# --- Logging ----------------------------------------------------------------

LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# --- Helpers ----------------------------------------------------------------

def partition_url(d: date) -> str:
    """Build the public URL for one day's billing CSV."""
    return f"{BUCKET_BASE}/year={d.year}/month={d.month:02d}/day={d.day:02d}/billing.csv"


def date_range(start: date, end: date):
    """Yield every date from start to end, inclusive."""
    days = (end - start).days
    for i in range(days + 1):
        yield start + timedelta(days=i)

# --- Database setup ---------------------------------------------------------

def setup_db(con: duckdb.DuckDBPyConnection) -> None:
    """Create the raw table and state table if they don't exist."""
    con.execute("INSTALL httpfs; LOAD httpfs;")

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_billing (
            timestamp      TIMESTAMP,
            resource_id    VARCHAR,
            user_id        BIGINT,
            credit_usage   DOUBLE,
            region         VARCHAR,
            service_tier   VARCHAR,
            operation_type VARCHAR,
            success        BOOLEAN,
            resource_type  VARCHAR,
            invoice_id     VARCHAR,
            currency       VARCHAR,
            year           INTEGER,
            month          INTEGER,
            day            INTEGER
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS processed_partitions (
            partition_date DATE PRIMARY KEY,
            url            VARCHAR,
            loaded_at      TIMESTAMP,
            row_count      INTEGER
        )
    """)

# --- State queries ----------------------------------------------------------

def get_processed_dates(con: duckdb.DuckDBPyConnection) -> set[date]:
    """Return the set of partition dates already loaded successfully."""
    rows = con.execute(
        "SELECT partition_date FROM processed_partitions"
    ).fetchall()
    return {row[0] for row in rows}

# --- Partition loading ------------------------------------------------------

def load_partition(con: duckdb.DuckDBPyConnection, d: date) -> int:
    """
    Load one day's partition into raw_billing and record it in state.

    Idempotent: re-running for the same date deletes existing rows for that
    date and reloads them. All three steps (delete, insert, state update)
    run in one transaction so a crash mid-load doesn't leave the warehouse
    in a partial state.

    Returns the number of rows loaded, or 0 if the partition is missing or
    fails to read.
    """
    url = partition_url(d)

    try:
        con.execute("BEGIN TRANSACTION")

        con.execute(
            "DELETE FROM raw_billing WHERE year = ? AND month = ? AND day = ?",
            [d.year, d.month, d.day],
        )

        con.execute(
            """
            INSERT INTO raw_billing
            SELECT * FROM read_csv(?, hive_partitioning = false)
            """,
            [url],
        )

        row_count = con.execute(
            "SELECT COUNT(*) FROM raw_billing WHERE year = ? AND month = ? AND day = ?",
            [d.year, d.month, d.day],
        ).fetchone()[0]

        con.execute(
            """
            INSERT OR REPLACE INTO processed_partitions
            VALUES (?, ?, ?, ?)
            """,
            [d, url, datetime.now(), row_count],
        )

        con.execute("COMMIT")
        log.info(f"Loaded {d} ({row_count} rows)")
        return row_count

    except Exception as e:
        con.execute("ROLLBACK")
        log.warning(f"Skipped {d}: {e}")
        return 0
    
# --- dbt invocation ---------------------------------------------------------

def run_dbt() -> None:
    """Run dbt build against the dbt_project folder."""
    log.info("Running dbt build")
    result = subprocess.run(
        ["dbt", "build",
        "--project-dir", str(DBT_PROJECT_DIR),
        "--profiles-dir", str(DBT_PROJECT_DIR)],
        capture_output=True,
        text=True,
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError("dbt build failed")


# --- Main -------------------------------------------------------------------

def main() -> None:
    con = duckdb.connect(str(DB_PATH))
    setup_db(con)

    processed = get_processed_dates(con)
    candidates = list(date_range(START_DATE, date.today()))
    # candidates = list(date_range(START_DATE, END_DATE)) # TEMP: smoke test
    new_partitions = [d for d in candidates if d not in processed]

    log.info(
        f"{len(candidates)} candidate partitions, "
        f"{len(processed)} already processed, "
        f"{len(new_partitions)} to load"
    )

    for d in new_partitions:
        load_partition(con, d)

    con.close()
    run_dbt()
    log.info("Pipeline complete")


if __name__ == "__main__":
    main()