"""
pipelines/load.py

Loads extracted stock price records into the Bronze layer
(raw_stock_prices) in Postgres.

Bronze is meant to be an append-only record of what was ingested —
minimal transformation, nothing overwritten. Cleaning/deduping
happens later in Silver (dbt), not here.
"""

import logging
import os

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw_stock_prices (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(12, 4) NOT NULL,
    high NUMERIC(12, 4) NOT NULL,
    low NUMERIC(12, 4) NOT NULL,
    close NUMERIC(12, 4) NOT NULL,
    volume BIGINT NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (ticker, date)
);
"""

INSERT_SQL = """
INSERT INTO raw_stock_prices (ticker, date, open, high, low, close, volume)
VALUES %s
ON CONFLICT (ticker, date) DO NOTHING;
"""


def get_connection():
    """
    Reads connection params from environment variables — populated
    from the .env file via Docker Compose / Airflow Connections.
    Never hardcoded here, on purpose (see the secrets-management
    section of the master prompt).
    """
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def load_to_bronze(records: list[dict]) -> None:
    """
    Bulk-inserts records into raw_stock_prices.

    Uses ON CONFLICT (ticker, date) DO NOTHING instead of a plain
    INSERT. This isn't for data-correctness reasons — extract.py's
    incremental filter already avoids re-fetching old dates. It's
    protection against the *task* retrying: if this function
    partially inserts a batch and then the task crashes for any
    reason, Airflow retries the load task with the same batch
    (records don't get re-extracted, just re-passed via XCom). A
    plain INSERT would throw a duplicate-key error or double the
    rows on that retry. ON CONFLICT DO NOTHING makes a retry safe.
    """
    if not records:
        logger.info("load_to_bronze called with 0 records — nothing to do")
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)

            values = [
                (r["ticker"], r["date"], r["open"], r["high"], r["low"], r["close"], r["volume"])
                for r in records
            ]
            execute_values(cur, INSERT_SQL, values)

        conn.commit()
        logger.info(f"Loaded {len(records)} record(s) into raw_stock_prices")

    except Exception:
        conn.rollback()
        logger.error("Failed to load records into raw_stock_prices — rolled back")
        raise  # re-raise so Airflow marks the task failed and actually retries

    finally:
        conn.close()
