"""
tests/test_extract.py

Unit tests for pipelines/extract.py -- runs without Airflow, Docker,
or a real network call. This is the actual payoff of keeping extract
logic out of the DAG file: it's testable like any other module.
"""

from unittest.mock import Mock, patch

import pytest

from pipelines.extract import extract_since, fetch_daily_prices

SAMPLE_SERIES = {
    "2026-08-05": {
        "1. open": "100.0", "2. high": "105.0", "3. low": "99.0",
        "4. close": "103.0", "5. volume": "1000000",
    },
    "2026-08-06": {
        "1. open": "103.0", "2. high": "108.0", "3. low": "102.0",
        "4. close": "107.0", "5. volume": "1200000",
    },
}


@patch("pipelines.extract.requests.get")
def test_fetch_daily_prices_returns_time_series(mock_get):
    mock_response = Mock()
    mock_response.json.return_value = {"Time Series (Daily)": SAMPLE_SERIES}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = fetch_daily_prices("AAPL", "fake_key")

    assert result["2026-08-06"]["4. close"] == "107.0"


@patch("pipelines.extract.requests.get")
def test_fetch_daily_prices_raises_on_rate_limit_message(mock_get):
    # Alpha Vantage returns HTTP 200 even on a rate-limit message --
    # the exact bug we hit and fixed. This test locks that fix in.
    mock_response = Mock()
    mock_response.json.return_value = {"Note": "rate limit exceeded"}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    with pytest.raises(ValueError, match="No time series data"):
        fetch_daily_prices("AAPL", "fake_key")


@patch("pipelines.extract.time.sleep")  # skip the real 15s delay in tests
@patch("pipelines.extract.fetch_daily_prices")
def test_extract_since_filters_by_last_fetched_date(mock_fetch, mock_sleep):
    mock_fetch.return_value = SAMPLE_SERIES

    records = extract_since(last_fetched_date="2026-08-05", api_key="fake_key")
    dates = {r["date"] for r in records if r["ticker"] == "AAPL"}

    assert "2026-08-06" in dates
    assert "2026-08-05" not in dates  # excluded -- not after the watermark


@patch("pipelines.extract.time.sleep")
@patch("pipelines.extract.fetch_daily_prices")
def test_extract_since_with_no_watermark_returns_everything(mock_fetch, mock_sleep):
    mock_fetch.return_value = SAMPLE_SERIES

    records = extract_since(last_fetched_date=None, api_key="fake_key")
    dates = {r["date"] for r in records if r["ticker"] == "AAPL"}

    assert dates == {"2026-08-05", "2026-08-06"}
