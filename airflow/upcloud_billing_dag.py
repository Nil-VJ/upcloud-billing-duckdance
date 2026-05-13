"""
Example Airflow DAG for the UpCloud billing pipeline.

This is an illustrative production wrapper, not a tested deployment. The DAG
is not run in this assignment — the pipeline is invoked directly by
ingestion/run_pipeline.py. In UpCloud's GCP environment this would run on
Composer (managed Airflow).

What the DAG does in production:
  1. Run the Python ingestion script to load new partitions into the warehouse.
  2. Run dbt build to construct the staging, intermediate, and marts layers
     and execute all tests.

Both tasks are idempotent, so retries on failure are safe.

PIPELINE_HOME is the deployment path of this repo on the Airflow worker,
set as an environment variable in the Composer environment config.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "bi-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="upcloud_billing_pipeline",
    description="Daily UpCloud billing ingestion and dbt build.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["billing", "duckdb", "dbt"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_partitions",
        bash_command="python ${PIPELINE_HOME}/ingestion/run_pipeline.py",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "dbt build "
            "--project-dir ${PIPELINE_HOME}/dbt_project "
            "--profiles-dir ${PIPELINE_HOME}/dbt_project"
        ),
    )

    ingest >> dbt_build