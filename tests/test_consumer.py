"""
tests/test_consumer.py

Unit tests for streaming/consumer.py -- the schema validation gate
and the AccountState rolling-average / velocity logic. No Kafka, no
real Postgres; the DB write in process_event is tested with a
mocked connection.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from consumer import AccountState, process_event
from schemas import TransactionEvent


def make_event(**overrides) -> dict:
    base = {
        "transaction_id": "txn-1",
        "account_id": "acct_0001",
        "merchant": "Amazon",
        "category": "groceries",
        "amount": 50.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location": "Chicago",
    }
    base.update(overrides)
    return base


def test_transaction_event_accepts_valid_data():
    assert TransactionEvent(**make_event()).account_id == "acct_0001"


def test_transaction_event_rejects_non_positive_amount():
    with pytest.raises(ValidationError):
        TransactionEvent(**make_event(amount=0))


def test_transaction_event_rejects_absurd_amount():
    with pytest.raises(ValidationError):
        TransactionEvent(**make_event(amount=5_000_000))


def test_account_state_rolling_average():
    state = AccountState()
    ts = datetime.now(timezone.utc)

    avg1, _ = state.record_and_compute("acct_0001", 100.0, ts)
    avg2, _ = state.record_and_compute("acct_0001", 200.0, ts)

    assert avg1 == 100.0
    assert avg2 == 150.0  # (100 + 200) / 2


def test_account_state_velocity_counts_within_window():
    state = AccountState()
    ts = datetime.now(timezone.utc)

    state.record_and_compute("acct_0001", 10.0, ts)
    _, velocity = state.record_and_compute("acct_0001", 10.0, ts + timedelta(seconds=5))

    assert velocity == 2  # both within the 60s window


def test_account_state_velocity_excludes_old_events():
    state = AccountState()
    ts = datetime.now(timezone.utc)

    state.record_and_compute("acct_0001", 10.0, ts)
    _, velocity = state.record_and_compute("acct_0001", 10.0, ts + timedelta(seconds=120))

    assert velocity == 1  # the first event aged out of the 60s window


def test_process_event_drops_invalid_json():
    conn = MagicMock()
    process_event(conn, AccountState(), b"not valid json")

    conn.cursor.assert_not_called()  # invalid events must never reach the DB


def test_process_event_writes_valid_event():
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value

    raw = TransactionEvent(**make_event()).model_dump_json().encode()
    process_event(conn, AccountState(), raw)

    assert cursor.execute.call_count == 2  # one Silver upsert, one Gold upsert
    conn.commit.assert_called_once()
