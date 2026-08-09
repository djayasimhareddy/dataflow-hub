"""
streaming/consumer.py

Reads transaction events from Redpanda, validates against the shared
schema, computes account-level analytics (rolling average spend,
transaction velocity, high-value alert), and writes to Postgres --
idempotently, via UPSERT on transaction_id, so a consumer restart or
message redelivery never double-counts.

Deliberately analytics-only, not fraud scoring: that logic already
exists in a separate project, so this stays data-engineering-focused
instead of overlapping it.

"Top merchants by volume" isn't stored as a per-row field here on
purpose -- it's a global aggregate, not a per-transaction fact, so
it belongs in a query at dashboard time (GROUP BY merchant) rather
than stamped onto every row.
"""

import logging
import os
from collections import defaultdict, deque
from datetime import datetime, timedelta

import psycopg2
from kafka import KafkaConsumer
from pydantic import ValidationError
from schemas import TransactionEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get("REDPANDA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = "transactions"

HIGH_VALUE_THRESHOLD = 500.0
VELOCITY_WINDOW_SECONDS = 60
ROLLING_AVG_COUNT = 5

CREATE_TABLES_SQL = """
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS silver.validated_transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    account_id VARCHAR(20) NOT NULL,
    merchant VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    location VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.transaction_analytics (
    transaction_id VARCHAR(64) PRIMARY KEY,
    account_id VARCHAR(20) NOT NULL,
    merchant VARCHAR(100) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    rolling_avg_amount NUMERIC(12, 2) NOT NULL,
    velocity_count INT NOT NULL,
    high_value_alert BOOLEAN NOT NULL,
    processed_at TIMESTAMP DEFAULT NOW()
);
"""

UPSERT_SILVER_SQL = """
INSERT INTO silver.validated_transactions
    (transaction_id, account_id, merchant, category, amount, event_timestamp, location)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO UPDATE SET
    account_id = EXCLUDED.account_id,
    merchant = EXCLUDED.merchant,
    category = EXCLUDED.category,
    amount = EXCLUDED.amount,
    event_timestamp = EXCLUDED.event_timestamp,
    location = EXCLUDED.location;
"""

UPSERT_GOLD_SQL = """
INSERT INTO gold.transaction_analytics
    (transaction_id, account_id, merchant, amount, event_timestamp,
     rolling_avg_amount, velocity_count, high_value_alert)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO UPDATE SET
    rolling_avg_amount = EXCLUDED.rolling_avg_amount,
    velocity_count = EXCLUDED.velocity_count,
    high_value_alert = EXCLUDED.high_value_alert;
"""


def get_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


class AccountState:
    """
    In-memory per-account history, used only to compute rolling
    average and velocity as events arrive. A real production system
    would use a stream-processing framework (Flink, ksqlDB) with
    proper windowing -- in-memory state in this one process is
    enough to demonstrate the concept at this scope. State resets on
    a consumer restart, which only affects these two derived fields,
    not the idempotent transaction record itself.
    """

    def __init__(self):
        self.recent_amounts: dict[str, deque] = defaultdict(lambda: deque(maxlen=ROLLING_AVG_COUNT))
        self.recent_timestamps: dict[str, deque] = defaultdict(deque)

    def record_and_compute(self, account_id: str, amount: float, ts: datetime) -> tuple[float, int]:
        self.recent_amounts[account_id].append(amount)
        rolling_avg = sum(self.recent_amounts[account_id]) / len(self.recent_amounts[account_id])

        window_start = ts - timedelta(seconds=VELOCITY_WINDOW_SECONDS)
        recent = self.recent_timestamps[account_id]
        recent.append(ts)
        while recent and recent[0] < window_start:
            recent.popleft()

        return round(rolling_avg, 2), len(recent)


def process_event(conn, state: AccountState, raw_value: bytes) -> None:
    try:
        event = TransactionEvent.model_validate_json(raw_value)
    except ValidationError as e:
        logger.error(f"Dropping invalid event: {e}")
        return

    rolling_avg, velocity_count = state.record_and_compute(
        event.account_id, event.amount, event.timestamp
    )
    high_value_alert = event.amount > HIGH_VALUE_THRESHOLD

    with conn.cursor() as cur:
        cur.execute(
            UPSERT_SILVER_SQL,
            (
                event.transaction_id, event.account_id, event.merchant,
                event.category, event.amount, event.timestamp, event.location,
            ),
        )
        cur.execute(
            UPSERT_GOLD_SQL,
            (
                event.transaction_id, event.account_id, event.merchant,
                event.amount, event.timestamp, rolling_avg, velocity_count, high_value_alert,
            ),
        )
    conn.commit()

    if high_value_alert:
        logger.warning(f"HIGH VALUE ALERT: {event.account_id} spent ${event.amount} at {event.merchant}")
    if velocity_count >= 3:
        logger.warning(
            f"VELOCITY ALERT: {event.account_id} has {velocity_count} txns "
            f"in the last {VELOCITY_WINDOW_SECONDS}s"
        )
    logger.info(f"Processed {event.transaction_id} | rolling_avg=${rolling_avg} | velocity={velocity_count}")


def main():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLES_SQL)
    conn.commit()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: v,  # keep raw bytes; Pydantic parses below
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="dataflow-hub-consumer",
    )
    logger.info(f"Consumer connected to {BOOTSTRAP_SERVERS}, reading '{TOPIC}'")

    state = AccountState()
    for message in consumer:
        process_event(conn, state, message.value)


if __name__ == "__main__":
    main()
