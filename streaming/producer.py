"""
streaming/producer.py

Generates synthetic transaction events and publishes them to the
Redpanda "transactions" topic. Validates against the shared Pydantic
schema before publishing -- the same contract the consumer expects,
which is the whole point of having a shared schemas.py.
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from pydantic import ValidationError
from schemas import TransactionEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MERCHANTS = ["Amazon", "Walmart", "Target", "Starbucks", "Uber", "Netflix", "Shell", "BestBuy"]
CATEGORIES = ["groceries", "electronics", "dining", "transport", "entertainment", "fuel"]
LOCATIONS = ["New York", "Chicago", "Austin", "Seattle", "Miami", "Denver"]
ACCOUNTS = [f"acct_{i:04d}" for i in range(1, 21)]  # 20 simulated accounts

BOOTSTRAP_SERVERS = os.environ.get("REDPANDA_BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = "transactions"


def generate_event() -> TransactionEvent:
    return TransactionEvent(
        transaction_id=str(uuid.uuid4()),
        account_id=random.choice(ACCOUNTS),
        merchant=random.choice(MERCHANTS),
        category=random.choice(CATEGORIES),
        amount=round(random.uniform(2, 800), 2),
        timestamp=datetime.now(timezone.utc),
        location=random.choice(LOCATIONS),
    )


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    logger.info(f"Producer connected to {BOOTSTRAP_SERVERS}, publishing to '{TOPIC}'")

    while True:
        try:
            event = generate_event()
        except ValidationError as e:
            # Won't happen with our own generator, but this is the same
            # gate a real upstream source would have to pass -- fail
            # loud instead of ever publishing something invalid.
            logger.error(f"Generated an invalid event, skipping: {e}")
            continue

        producer.send(TOPIC, value=event.model_dump(mode="json"))
        logger.info(f"Published {event.transaction_id} | {event.account_id} | ${event.amount}")

        time.sleep(random.uniform(0.5, 2.0))  # irregular arrival, not a fixed tick


if __name__ == "__main__":
    main()
