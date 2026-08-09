"""
airflow/dags/batch_pipeline_dag.py

Thin DAG definition on purpose. All real logic lives in pipelines/,
great_expectations/, and dbt/ -- this file only wires tasks together
and sets scheduling/retry behavior.

Flow: extract -> load (Bronze) -> validate (GE, Bronze only) ->
dbt run + dbt test (Silver/Gold). If GE fails, dbt never runs on bad
raw data. If a dbt test fails, the task fails -- no silent pass.
"""

import logging
import sys
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator

sys.path.append("/opt/airflow")

from data_quality.validate_bronze import validate_bronze
from pipelines.extract import extract_since
from pipelines.load import load_to_bronze

logger = logging.getLogger(__name__)

default_args = {
    "owner": "jayasimha",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="batch_pipeline_dag",
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["dataflow-hub", "batch"],
)
def batch_pipeline():
    @task
    def extract() -> list[dict]:
        api_key = Variable.get("ALPHA_VANTAGE_API_KEY")
        last_fetched_date = Variable.get("last_fetched_date", default_var=None)

        records = extract_since(last_fetched_date, api_key)

        if records:
            latest_date = max(r["date"] for r in records)
            Variable.set("last_fetched_date", latest_date)

        return records

    @task
    def load(records: list[dict]) -> None:
        if not records:
            logger.info("No new records -- skipping load")
            return
        load_to_bronze(records)

    @task
    def validate() -> None:
        validate_bronze()

    dbt_run_and_test = BashOperator(
        task_id="dbt_run_and_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --profiles-dir . && "
            "dbt test --profiles-dir ."
        ),
    )

    load(extract()) >> validate() >> dbt_run_and_test


batch_pipeline()