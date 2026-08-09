# DataFlow Hub

A Dockerized data platform combining a batch ETL pipeline (Airflow + dbt) ingesting daily stock prices, and a streaming pipeline (Redpanda) processing simulated payment transactions. Both land in a shared, layered (Bronze/Silver/Gold) Postgres database, surfaced through a Streamlit dashboard.

Built as a portfolio project for Data Engineer / Data Pipeline roles.

## Architecture

```
BATCH                                          STREAMING
─────                                          ─────────
Alpha Vantage API (daily stock prices)         Simulated transaction events
        │  incremental (last_fetched_date)             │  validated: Pydantic schema
        ▼                                              ▼
   Airflow DAG                                   Redpanda topic
   extract → load                                      │
        │                                              ▼
        ▼                                        Python consumer
   BRONZE (raw_stock_prices)                      → rolling avg / velocity /
        │  dbt run                                  high-value alert
        ▼                                              │  UPSERT on transaction_id
   SILVER (clean_stock_prices)                        ▼
        │  dbt run + dbt test                    SILVER (validated_transactions)
        ▼                                              │
   GOLD (daily_stock_summary)                          ▼
        │                                        GOLD (transaction_analytics)
        └──────────────────┬─────────────────────────┘
                            ▼
                   Postgres (single instance,
                   shared with Airflow's own metadata)
                            ▼
                   Streamlit Dashboard
              (batch health tab + live analytics tab)
```

A second DAG, `weekly_cleanup_dag`, prunes Bronze rows older than 180 days on a weekly schedule — included to demonstrate orchestration beyond a single workflow.

## Tech stack

| Layer | Tool | Why |
|---|---|---|
| Orchestration | Apache Airflow (LocalExecutor) | retries + logging + scheduling for the batch DAG |
| Transformation | dbt-postgres | Silver/Gold models, dbt-native tests |
| Raw data quality | Great Expectations | validates Bronze only — doesn't overlap with dbt tests |
| Streaming broker | Redpanda | Kafka-API compatible, lighter to run locally |
| Event contract | Pydantic | shared schema between producer and consumer |
| Database | PostgreSQL | single instance, Bronze/Silver/Gold via schemas |
| Dashboard | Streamlit | batch health + live streaming analytics |
| Tests | pytest | extract.py and consumer.py logic |
| CI | GitHub Actions | lint (ruff) → pytest → docker build |

**Cloud equivalents** (not deployed, for context): Airflow → Cloud Composer / MWAA, Redpanda → AWS MSK / Confluent Cloud, Postgres → RDS / Cloud SQL.

## Data sources

- **Batch**: Alpha Vantage free API (`TIME_SERIES_DAILY`), 5 fixed tickers. The free tier is genuinely tight — 1 request/second and 25 requests/day — so extraction is incremental (tracks `last_fetched_date`) and throttled between calls.
- **Streaming**: a synthetic transaction generator (`streaming/producer.py`) — no external API needed.

## Setup

```bash
cp .env.example .env        # fill in a Postgres password
docker compose up -d --build
docker compose exec airflow-webserver airflow variables set ALPHA_VANTAGE_API_KEY your_key_here
```

- Airflow UI: `http://localhost:8080` (admin/admin)
- Streamlit dashboard: `http://localhost:8501`

Unpause and trigger `batch_pipeline_dag` from the Airflow UI. Streaming (`producer`/`consumer`) runs continuously once started — stop it when not actively demoing:

```bash
docker compose stop producer consumer
```

## Folder structure

```
dataflow-hub/
├── docker-compose.yml, Dockerfile, requirements.txt   # Airflow image
├── airflow/dags/            # batch_pipeline_dag.py, weekly_cleanup_dag.py
├── pipelines/                # extract.py, load.py — plain Python, unit-testable
├── data_quality/             # validate_bronze.py — Great Expectations, Bronze only
├── dbt/                      # Silver/Gold models, schema tests, schema-naming macro
├── streaming/                 # schemas.py, producer.py, consumer.py, own Dockerfile
├── dashboard/                 # app.py — Streamlit, own Dockerfile
├── tests/                     # pytest — extract.py and consumer.py
└── .github/workflows/ci.yml
```

## Data modeling — Bronze / Silver / Gold

- **Bronze**: `raw_stock_prices` — append-only, `ON CONFLICT DO NOTHING` (idempotent against Airflow task retries)
- **Silver**: `silver.clean_stock_prices`, `silver.validated_transactions` — deduplicated / schema-validated
- **Gold**: `gold.daily_stock_summary` (daily return %, 7-day moving average), `gold.transaction_analytics` (rolling average spend, transaction velocity, high-value alerts)

Streaming writes use `ON CONFLICT (transaction_id) DO UPDATE` — idempotent against consumer restarts or message redelivery.

## Design decisions worth knowing

- **Great Expectations validates Bronze only; dbt tests validate Silver/Gold.** Running both on the same table would be redundant tooling, not extra rigor.
- **No fraud-scoring ML in the streaming consumer**, on purpose — that logic lives in a separate project. This one stays analytics-only (rolling average, velocity, threshold alerts) so the two don't overlap on a resume.
- **"Top merchants by volume" is a dashboard-time `GROUP BY` query, not a stored column** — it's a global aggregate, not a per-transaction fact.
- **The dbt schema-naming macro** (`dbt/macros/generate_schema_name.sql`) overrides dbt's default behavior, which would otherwise create `public_silver`/`public_gold` instead of clean `silver`/`gold` schemas.
- **`data_quality/`, not `great_expectations/`** — naming the folder after the installed pip package it depends on caused a real import collision during development; renaming it was the fix.

## Future improvements (documented, not built)

- **MinIO data lake landing zone** for raw files before they hit Postgres — skipped to avoid a 4th local infrastructure service for a project already running 6+ containers.
- **Avro/Protobuf + Confluent Schema Registry** for streaming — the Pydantic shared-schema approach covers the same interview point without an extra service.
- **Prometheus/Grafana observability** — Airflow's UI + structured logs + the dashboard's own metrics are sufficient at this scope.
- Parsing dbt's `run_results.json` to surface per-test pass/fail detail in the dashboard, instead of using the Airflow task state as a proxy signal.
- Giving Airflow its own dedicated metadata database, separate from the Bronze/Silver/Gold data — currently they share one Postgres instance, which is fine locally but not how a production setup would be split.

## Lessons learned

- Alpha Vantage returns HTTP 200 even on rate-limit messages — `response.raise_for_status()` alone doesn't catch it; the response body has to be checked explicitly.
- A local folder sharing a name with an installed pip package (`great_expectations/`) silently breaks imports — Python resolves the installed package first.
- `_PIP_ADDITIONAL_REQUIREMENTS` reinstalling packages on every container start is fragile (hit a flaky network failure mid-install); switching to a custom-built image via `Dockerfile` fixed it and made startup faster besides.
- dbt's default schema-naming behavior concatenates the profile's default schema with any custom schema config (`public_silver`, not `silver`) unless overridden with a `generate_schema_name` macro.
