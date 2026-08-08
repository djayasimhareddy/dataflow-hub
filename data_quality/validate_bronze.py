"""
great_expectations/validate_bronze.py

Validates Bronze (raw_stock_prices) right after load, before dbt
touches it -- the ingestion quality gate. dbt tests (in
dbt/models/silver and dbt/models/gold) handle transformation
correctness separately, so GE and dbt aren't checking the same thing.

Note: Great Expectations' Python API has shifted across versions.
This uses the current GX Core ("Fluent") API. If a method name here
doesn't match your installed version, check docs.greatexpectations.io
for the equivalent in your version.
"""

import logging
import os

import great_expectations as gx
import pandas as pd
import psycopg2

logger = logging.getLogger(__name__)


def _fetch_bronze_as_dataframe() -> pd.DataFrame:
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", 5432),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        return pd.read_sql("SELECT * FROM raw_stock_prices", conn)
    finally:
        conn.close()


def validate_bronze() -> None:
    """Raises if any expectation fails, so the calling Airflow task
    fails loudly instead of letting dbt build on top of bad raw data."""
    df = _fetch_bronze_as_dataframe()

    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas("bronze_pandas")
    asset = source.add_dataframe_asset(name="raw_stock_prices")
    batch_def = asset.add_batch_definition_whole_dataframe("bronze_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    checks = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="ticker"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="date"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="close"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="close", min_value=0, strict_min=True),
        gx.expectations.ExpectColumnValuesToBeBetween(column="volume", min_value=0),
    ]

    failures = []
    for expectation in checks:
        result = batch.validate(expectation)
        (logger.info if result.success else logger.error)(
            f"GE {'passed' if result.success else 'FAILED'}: {expectation}"
        )
        if not result.success:
            failures.append(str(expectation))

    if failures:
        raise ValueError(f"Bronze validation failed: {failures}")

    logger.info(f"Bronze validation passed on {len(df)} row(s)")