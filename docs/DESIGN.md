# Design

## Why this shape

OTT ads analytics dies in two places: (1) double-counting on reprocessing, (2) floats used as money. Everything else is taste.

## Layers

| Layer | Contract |
|-------|----------|
| **Bronze** | Immutable JSONL as received. Partition `dt=YYYY-MM-DD`. Quarantine poison rows. |
| **Silver** | Typed, deduped Parquet. One row per `event_id`. Overwrite by `dt`. |
| **Gold** | Business grains: `fct_campaign_daily`, `fct_pod_fill`. |

## Idempotency

- Airflow `ds` selects the partition.
- Silver overwrite for that `dt` only.
- Gold writers drop existing `event_date = ds` then append.
- Re-run must not inflate revenue.

## Late data

- Generator marks ~2% impressions `is_late=true` with event_ts skewed forward.
- In production you'd land late files into the *arrival* dt and merge; here we keep the demo honest without a full CDC story.

## Partitioning

```
data/bronze/ad_events/dt=YYYY-MM-DD/*.jsonl
data/silver/ad_events/dt=YYYY-MM-DD/*.parquet
data/gold/fct_campaign_daily.parquet
data/gold/fct_pod_fill.parquet
```

Cluster keys if this were Snowflake/BigQuery: `campaign_id`, `market`.

## Failure modes

- Schema fail → quarantine, not silent drop
- GE gate fails the DAG (no “warn and continue” for gold publish)
- Airflow retries 2x with backoff

## Spark vs pandas

Laptop path = pandas/pyarrow silver (same columns). Cluster path = `spark_sql_silver.py`. Don't maintain two *different* business rules — same SQL/transform contract.
