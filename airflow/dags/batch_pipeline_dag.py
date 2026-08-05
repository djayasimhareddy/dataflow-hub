"""
airflow/dags/batch_pipeline_dag.py

Thin DAG definition on purpose. All real logic lives in pipelines/ —
this file only wires tasks together and sets scheduling/retry
behavior. That split is what keeps extract.py unit-testable without
Airflow running, and it's a real interview point: "why isn't your
logic just written inside the DAG file?"

NOTE: load.py (pipelines/load.py) is referenced below but not built
yet — that's the next piece. This DAG will fail on the load step
until it exists, which is expected at this stage.
"""

import logging
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

# Adjust this path to wherever docker-compose mounts the repo root
# inside the Airflow container (we'll set that up when we write
# docker-compose.yml).
sys.path.append("/opt/airflow")

from pipelines.extract import extract_since
from pipelines.load import load_to_bronze  # next piece — not built yet

logger = logging.getLogger(__name__)

default_args = {
    "owner": "jayasimha",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="batch_pipeline_dag",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,  # don't backfill every day since 2026-01-01 on first deploy
    default_args=default_args,
    tags=["dataflow-hub", "batch"],
)
def batch_pipeline():
    @task
    def extract() -> list[dict]:
        api_key = Variable.get("ALPHA_VANTAGE_API_KEY")
        # default_var=None handles the very first run, before any
        # watermark has been set
        last_fetched_date = Variable.get("last_fetched_date", default_var=None)

        records = extract_since(last_fetched_date, api_key)

        if records:
            # Advance the watermark only AFTER a successful extract.
            # If this task fails partway, the retry re-uses the old
            # date instead of silently skipping records.
            latest_date = max(r["date"] for r in records)
            Variable.set("last_fetched_date", latest_date)

        return records

    @task
    def load(records: list[dict]) -> None:
        if not records:
            logger.info("No new records — skipping load")
            return
        load_to_bronze(records)

    load(extract())


batch_pipeline()
