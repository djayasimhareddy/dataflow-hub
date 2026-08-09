"""
dashboard/app.py

Two tabs:
- Batch Pipeline Health: latest Airflow task states (read directly
  from Airflow's own metadata tables -- fair game since it shares
  this same Postgres instance) + row counts per Bronze/Silver/Gold
  layer + a stock price chart.
- Live Transaction Analytics: auto-refreshing view of the streaming
  layer's Gold output, throughput, and an alerts feed.
"""

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="DataFlow Hub", layout="wide")
DB_EXCEPTIONS = (psycopg2.Error, KeyError, IndexError, TypeError, ValueError)


def get_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def run_query(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()


st.title("DataFlow Hub")

tab1, tab2 = st.tabs(["Batch Pipeline Health", "Live Transaction Analytics"])

with tab1:
    st.subheader("Latest Airflow task run status")
    try:
        task_states = run_query(
            """
            SELECT DISTINCT ON (task_id) task_id, state, end_date
            FROM task_instance
            WHERE dag_id = 'batch_pipeline_dag'
            ORDER BY task_id, end_date DESC NULLS LAST
            """
        )
        if task_states.empty:
            st.info("No DAG runs yet -- trigger batch_pipeline_dag in the Airflow UI first.")
        else:
            cols = st.columns(len(task_states))
            for col, (_, row) in zip(cols, task_states.iterrows()):
                color = {"success": "green", "failed": "red"}.get(row["state"], "orange")
                col.markdown(f"**{row['task_id']}**")
                col.markdown(f":{color}[{row['state']}]")
    except DB_EXCEPTIONS as e:
        st.warning(f"Could not read Airflow task state: {e}")

    st.subheader("Row counts per layer")
    c1, c2, c3 = st.columns(3)
    try:
        c1.metric("Bronze -- raw_stock_prices", int(run_query("SELECT COUNT(*) AS n FROM raw_stock_prices")["n"][0]))
    except DB_EXCEPTIONS:
        c1.metric("Bronze -- raw_stock_prices", "n/a")
    try:
        c2.metric("Silver -- clean_stock_prices", int(run_query("SELECT COUNT(*) AS n FROM silver.clean_stock_prices")["n"][0]))
    except DB_EXCEPTIONS:
        c2.metric("Silver -- clean_stock_prices", "n/a")
    try:
        c3.metric("Gold -- daily_stock_summary", int(run_query("SELECT COUNT(*) AS n FROM gold.daily_stock_summary")["n"][0]))
    except DB_EXCEPTIONS:
        c3.metric("Gold -- daily_stock_summary", "n/a")

    st.subheader("Closing price by ticker")
    try:
        prices = run_query("SELECT ticker, date, close FROM gold.daily_stock_summary ORDER BY date")
        if not prices.empty:
            st.line_chart(prices.pivot(index="date", columns="ticker", values="close"))
        else:
            st.info("No Gold data yet -- trigger the batch DAG.")
    except DB_EXCEPTIONS as e:
        st.warning(f"Could not load chart: {e}")

    st.caption(
        "Note: dbt test pass/fail and GE validation results aren't pulled in here directly -- "
        "the Airflow task state above (validate / dbt_run_and_test) is the proxy signal, since "
        "both raise and fail their task on any check failure. Parsing dbt's run_results.json for "
        "per-test detail is a documented future improvement, not built."
    )

with tab2:
    st_autorefresh(interval=5000, key="live_refresh")

    st.subheader("Throughput")
    try:
        window_start = datetime.now(timezone.utc) - timedelta(seconds=30)
        recent = run_query(
            "SELECT COUNT(*) AS n FROM gold.transaction_analytics WHERE processed_at >= %s",
            params=(window_start,),
        )
        st.metric("Events/sec (last 30s)", round(recent["n"][0] / 30, 2))
    except DB_EXCEPTIONS as e:
        st.warning(f"Could not compute throughput: {e}")

    st.subheader("Live transaction feed")
    try:
        live = run_query(
            """
            SELECT transaction_id, account_id, merchant, amount,
                   rolling_avg_amount, velocity_count, high_value_alert, processed_at
            FROM gold.transaction_analytics
            ORDER BY processed_at DESC
            LIMIT 25
            """
        )
        st.dataframe(live, use_container_width=True)
    except DB_EXCEPTIONS as e:
        st.warning(f"Could not load live feed: {e}")

    st.subheader("Alerts (high-value or high-velocity)")
    try:
        alerts = run_query(
            """
            SELECT transaction_id, account_id, merchant, amount,
                   velocity_count, high_value_alert, processed_at
            FROM gold.transaction_analytics
            WHERE high_value_alert = true OR velocity_count >= 3
            ORDER BY processed_at DESC
            LIMIT 20
            """
        )
        if alerts.empty:
            st.info("No alerts yet -- start the producer/consumer to generate live traffic.")
        else:
            st.dataframe(alerts, use_container_width=True)
    except DB_EXCEPTIONS as e:
        st.warning(f"Could not load alerts: {e}")

    st.subheader("Top merchants by volume")
    try:
        top_merchants = run_query(
            """
            SELECT merchant, COUNT(*) AS transaction_count
            FROM gold.transaction_analytics
            GROUP BY merchant
            ORDER BY transaction_count DESC
            LIMIT 10
            """
        )
        st.bar_chart(top_merchants.set_index("merchant"))
    except DB_EXCEPTIONS as e:
        st.warning(f"Could not load top merchants: {e}")
