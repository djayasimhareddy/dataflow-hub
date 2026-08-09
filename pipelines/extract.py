"""
pipelines/extract.py

Pulls daily stock price data from Alpha Vantage.

Incremental by design: only returns bars newer than the last
successful run. This isn't just an interview talking point here —
Alpha Vantage's free tier is genuinely tight: 1 request/second and
25 requests/day, confirmed from the API's own rate-limit response.
A full re-pull every run would burn through that budget in a
single call.

Lives outside the DAG file on purpose: Airflow doesn't need to be
running for you to test this function.
"""

import logging
import time

import requests

logger = logging.getLogger(__name__)

ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

# Fixed, small set of tickers — deliberate choice to respect the free
# rate limit (5 tickers x 1 call each stays well under 5 calls/min).
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]


def fetch_daily_prices(ticker: str, api_key: str) -> dict:
    """
    Calls Alpha Vantage TIME_SERIES_DAILY for one ticker.

    Raises on failure (bad HTTP status, or a malformed response body)
    instead of returning an empty result — this is what lets Airflow's
    retry logic actually do something useful. A function that fails
    silently gives you a pipeline that "succeeds" with no data.
    """
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "apikey": api_key,
        "outputsize": "compact",  # last ~100 days — plenty for a daily incremental pull
    }
    response = requests.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "Time Series (Daily)" not in data:
        # Alpha Vantage returns HTTP 200 even when it's actually giving you
        # a rate-limit message or an error string in the body. Checking
        # response.ok is not enough — this is the check that actually
        # catches it, instead of silently loading garbage into Bronze.
        reason = data.get("Note") or data.get("Information") or data
        logger.error(f"Unexpected response for {ticker}: {reason}")
        raise ValueError(f"No time series data for {ticker}: {reason}")

    return data["Time Series (Daily)"]


def extract_since(last_fetched_date: str | None, api_key: str) -> list[dict]:
    """
    Extracts daily bars for all TICKERS, filtered to dates strictly
    after last_fetched_date.

    last_fetched_date: ISO date string like "2026-08-01", or None on
    the very first run (in which case everything in the compact
    window is returned).

    Returns a flat list of dicts, one per (ticker, date) — ready to
    hand to load.py for the Bronze insert.
    """
    all_records: list[dict] = []

    for i, ticker in enumerate(TICKERS):
        if i > 0:
            # Free tier throttles at 1 request/second. This is the fix
            # for the actual failure you hit -- looping through 5
            # tickers with no delay hit that limit on the 2nd call.
            time.sleep(15)

        logger.info(f"Fetching {ticker} from Alpha Vantage")
        daily_series = fetch_daily_prices(ticker, api_key)

        new_count = 0
        for day_str, values in daily_series.items():
            if last_fetched_date and day_str <= last_fetched_date:
                continue  # already ingested — this line IS the incremental load

            all_records.append(
                {
                    "ticker": ticker,
                    "date": day_str,
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": int(values["5. volume"]),
                }
            )
            new_count += 1

        if new_count == 0:
            logger.warning(f"No new records for {ticker} since {last_fetched_date}")
        else:
            logger.info(f"{ticker}: {new_count} new record(s)")

    logger.info(f"Extract complete: {len(all_records)} total new record(s)")
    return all_records