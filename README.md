# upcloud-billing-duckdance

A small billing data pipeline built for the UpCloud Senior Data Engineer assignment. Reads Hive-partitioned CSVs from UpCloud Object Storage, transforms them with dbt, and produces aggregated marts. Documentation, trade-offs, and a discussion of what production would look like are in this README.

## Setup

## Architecture

The pipeline has four layers: storage, compute, transformation, and orchestration. Each layer was picked to keep the architecture light and the dependencies few. The assignment explicitly asks for this and I agree with it for a dataset of this size.

### Storage

Source data lives in UpCloud Object Storage as Hive-partitioned CSVs at `year=YYYY/month=MM/day=DD/billing.csv`. The bucket allows public HTTPS reads of specific files but rejects anonymous S3 LIST, so a glob like `year=*/month=*/day=*/billing.csv` does not work. I generate the list of partition URLs in Python and pass them to DuckDB explicitly. This is closer to how a real incremental pipeline behaves anyway, since you would not want to LIST the full bucket on every run.

Marts are materialized as CSV files in a local outputs folder and as tables inside the DuckDB file. CSV because I assumed the downstream consumers here are humans — finance and BI analysts — who can open it directly. In production these would land in BigQuery as tables, and the local CSVs would be written back to UpCloud Object Storage through DuckDB's httpfs extension.

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

## Data lineage and catalog

## GDPR considerations

## Time spent
