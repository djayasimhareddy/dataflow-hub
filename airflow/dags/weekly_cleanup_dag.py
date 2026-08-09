"""
airflow/dags/weekly_cleanup_dag.py

Second DAG, running weekly instead of daily. Exists to demonstrate
orchestration beyond a single workflow -- prunes Bronze rows older
than a retention window so raw_stock_prices doesn't grow unbounded.

Deliberately simple: one task, no branching. The point isn't
complexity, it's showing a second independently-scheduled DAG in the
same project.
"""

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

RETENTION_DAYS = 180  # keep roughly 6 months of raw daily bars

default_args = {
    "owner": "jayasimha",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="weekly_cleanup_dag",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dataflow-hub", "maintenance"],
)
def weekly_cleanup():
    @task
    def prune_old_bronze_rows() -> None:
        import os

        import psycopg2

        conn = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ.get("POSTGRES_PORT", 5432),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM raw_stock_prices
                    WHERE date < CURRENT_DATE - INTERVAL '%s days'
                    """,
                    (RETENTION_DAYS,),
                )
                deleted = cur.rowcount
            conn.commit()
            logger.info(f"Pruned {deleted} row(s) older than {RETENTION_DAYS} days")
        except Exception:
            conn.rollback()
            logger.error("Cleanup failed -- rolled back")
            raise
        finally:
            conn.close()

    prune_old_bronze_rows()


weekly_cleanup()
