# upcloud-billing-duckdance

A small billing data pipeline built for the UpCloud Senior Data Engineer assignment. Reads Hive-partitioned CSVs from UpCloud Object Storage, transforms them with dbt, and produces aggregated marts. Documentation, trade-offs, and a discussion of what production would look like are in this README.

## Setup

## Architecture

The pipeline has four layers: storage, compute, transformation, and orchestration. Each layer was picked to keep the architecture light and the dependencies few. The assignment explicitly asks for this and I agree with it for a dataset of this size.

### Storage

Source data lives in UpCloud Object Storage as Hive-partitioned CSVs at `year=YYYY/month=MM/day=DD/billing.csv`. The bucket allows public HTTPS reads of specific files but rejects anonymous S3 LIST, so a glob like `year=*/month=*/day=*/billing.csv` does not work. I generate the list of partition URLs in Python and pass them to DuckDB explicitly. This is closer to how a real incremental pipeline behaves anyway, since you would not want to LIST the full bucket on every run.

Marts are materialized as tables inside the DuckDB file. A reviewer can open the file with the DuckDB CLI or any DuckDB-compatible tool and query the marts directly with SQL. In production these would land in BigQuery as tables, and the same dbt models would point at the BigQuery profile instead of the DuckDB profile.

### Compute: DuckDB

DuckDB reads CSVs directly from the bucket over HTTPS using the `httpfs` extension and runs SQL aggregations in-process. No server, no containers, no separate engine to manage. For a single-developer pipeline against a few hundred megabytes per partition, this is the right tool. It would not scale to terabytes, but in here it is not needed.

The honest trade-off: DuckDB is fast for analytical queries but is single-node. If the dataset grew enough that one machine could not hold the working set, I would move the compute to BigQuery or a Spark cluster. For this assignment, DuckDB is the right tool here.

### Transformation: dbt with the dbt-duckdb adapter

dbt structures the SQL transformations into staging, intermediate, and marts layers. Three reasons I chose this over plain SQL scripts:

1. Tests come for free. Schema tests like `not_null` and `unique` and custom data tests run with `dbt test`.
2. Lineage comes for free. `dbt docs generate` builds a model-level DAG you can browse.
3. The pattern matches what UpCloud already uses. The job description mentions dbt explicitly.

The dbt-duckdb adapter is the bridge. It is community-maintained, and lets dbt treat DuckDB as a warehouse.

### Orchestration: Python script now, Airflow DAG for production

For the 3-hour build, orchestration is a Python script with a `main()` entry point. It does: discover new partitions, load them into DuckDB, run dbt, log the result.

I did not run Airflow locally because the setup overhead is high and the assignment caps build time at 3 hours. Instead I included one example Airflow DAG file in `airflow/` that shows how this would be wrapped in production. In UpCloud's case that would run on Composer, the managed Airflow on GCP.

This split is deliberate. A Python script is honest about what the pipeline actually does at its core. The Airflow DAG is honest about what changes for production: scheduling, retries, alerting, dependencies between tasks.

### Why not the obvious alternatives

A few choices I considered and rejected, in case it comes up:

- **pandas instead of DuckDB.** Works for small data but slower and uses more memory. DuckDB also gives me SQL, which the rest of the pipeline (dbt) needs anyway.
- **Spark or Dask.** Massive overkill for this dataset. Heavy dependencies, slow startup, harder to reason about.
- **Cloud-managed ETL (Dataflow, etc.).** The assignment explicitly says to prefer open source over expensive cloud services.
- **Just SQL scripts, no dbt.** Possible, but I would lose tests, lineage, and documentation. dbt's overhead is small.

## Data model

## Idempotency

## Running the pipeline

Clone the repo and create a virtual environment with Python 3.12. Install dependencies: `pip install -r requirements.txt`

Then run the pipeline from the repo root: `python ingestion/run_pipeline.py`

This loads any new partitions from the bucket into a local `warehouse.duckdb` file, then runs `dbt build` to construct the staging, intermediate, and marts layers and execute all tests. The full chain runs end to end in one command.

To inspect the output, open the DuckDB file from Python: `python -c "import duckdb; con = duckdb.connect('warehouse.duckdb'); print(con.execute('select * from mart_daily_usage_by_region limit 10').fetchdf())"`

Re-running the pipeline is safe. Partitions that have already been loaded are skipped, and dbt rebuilds the marts against whatever data is currently in `raw_billing`.

### A note on 404s

The bucket adds new partitions over time. The pipeline tries every date from `START_DATE` to today, and partitions that do not yet exist return HTTP 404. These show up in the log as `WARNING Skipped YYYY-MM-DD: HTTP 404` and are expected. The next run picks them up automatically once the file appears in the bucket.

## Data lineage and catalog

## GDPR considerations

The dataset contains a `user_id` column, which is personal data under GDPR even though it is a number rather than a name. A pipeline that handles billing data needs to take it seriously.

### Data classification

Looking at the 14 columns:

- **Personal data:** `user_id`. Even though it is pseudonymous, it identifies an individual when combined with other systems. Under GDPR this still counts as personal data.
- **Possibly personal in context:** `resource_id` and `invoice_id`. On their own they identify resources or transactions, not people. Joined with `user_id`, they become traceable to individuals.
- **Not personal:** timestamp, credit_usage, region, service_tier, operation_type, success, resource_type, currency, year, month, day.

The practical implication for this pipeline: raw and staging tables contain user_id and are in scope for GDPR. The aggregated marts group by dimensions like region, service_tier, and day, and drop user_id entirely. Those marts are out of GDPR scope because no individual can be identified from a row like '(us-chi1, 2024-01-01, -88M total credits)'. This is a deliberate design choice: aggregation is itself a form of GDPR data minimization.

### Pseudonymization

`user_id` looks already pseudonymous in this dataset (it is an integer, not an email or name). For marts, I aggregate by dimensions that do not include `user_id` whenever the use case allows. Where per-user data is needed, the right pattern is to keep `user_id` only in restricted tables and grant access only to roles that need it.

### Retention and right to erasure

Two tensions worth naming.

**Retention.** Billing records and personal data want different lifespans. Billing records often need to be kept for years for audit and finance reasons. Personal data inside those records (the `user_id` column) should only be kept as long as the original purpose requires. The pipeline does not currently enforce retention rules. In production, a scheduled job would either delete or anonymize raw partitions older than the retention threshold, while keeping the aggregated marts intact.

**Right to erasure.** If a user requests deletion, the raw partitions can be filtered to remove their rows. Aggregated marts are harder because that user's data is mixed into sums and averages across millions of others. The pragmatic answer: keep the raw data around so the marts can be recomputed if needed. For aggregates over very large groups, the values are anonymous enough that erasure typically does not apply to them.

### Data residency

UpCloud's value proposition includes European data sovereignty. For this pipeline, that means: source data stays in UpCloud Object Storage (European regions), the pipeline runs in European regions, and outputs land in European regions. DuckDB running on a developer's laptop is fine for this assignment, but in production the compute would run on European infrastructure too. This is not just a GDPR checkbox; it is the product story.

## Time spent
