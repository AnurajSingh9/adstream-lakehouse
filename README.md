# AdStream

OTT AdTech insights lakehouse.

I built this around the kind of work you actually hit on a streaming platform: mid-roll pods that never get filled, SSAI beacons that arrive late, campaign rollups that disagree with finance by a few rupees, and Spark jobs that quietly double-count if you re-run yesterday.

Not a generic “medallion demo”. The grain, event names, and marts are AdTech-shaped.

---

## What problem it solves

Product / ads analytics asks questions like:

- What was **fill rate** and **eCPM** for campaign X on Android in IN yesterday?
- How often did mid-roll pods **break** (request → no impression)?
- Are we leaking money because **completion** events don’t line up with **impression** events?
- Can we re-run `ds=2026-09-18` without inflating gold?

AdStream lands raw beacons, cleans them in Spark, builds dbt marts, and fails the DAG if Great Expectations says the day is junk.

```
  Kafka topics (ad.request / impression / click / quartile / error)
            │
            ▼
     S3/GCS bronze  (immutable JSONL, dt=)
            │
            ▼
   Spark (PySpark + Spark SQL)  →  silver Delta/Parquet
            │
            ▼
        dbt models  →  gold marts (Postgres locally / Snowflake-shaped)
            │
            ▼
   Great Expectations gate  →  Airflow marks success / fails loud
            │
            ▼
      FastAPI read API  (campaign daily, pod fill)
```

---

## Stack

| Layer | Choice |
|-------|--------|
| Streaming land | Kafka (local) → JSONL partitions |
| Lake | S3/GCS layout, Parquet + Delta-style merge semantics |
| Compute | Spark (PySpark / Spark SQL); Scala job stub for the hot path |
| Orchestration | Airflow |
| Transform SQL | dbt |
| Quality | Great Expectations |
| Serving | FastAPI + Postgres |
| Ops | Docker Compose, Make, GitHub Actions |

---

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

make sample          # synthetic OTT ad beacons
make silver          # bronze → silver
make dbt-run         # staging → marts (duckdb locally)
make ge              # quality gate
make test
make api             # http://127.0.0.1:8088/docs
```

No cloud account needed for the local path. AWS / GCS mapping: [docs/CLOUD_MAPPING.md](docs/CLOUD_MAPPING.md).

---

## Layout

```
airflow/dags/          daily + catchup-safe DAG
pipelines/
  ingest/              land + kafka seeder
  spark/               bronze→silver, gold prep
  common/              paths, schemas, money helpers
dbt/adstream/          staging / intermediate / marts
great_expectations/    silver + gold suites
api/                   FastAPI over gold tables
schemas/               event contracts (JSON Schema)
docs/                  design notes, ADRs, domain glossary
scripts/               sample generator, backfill helper
tests/
scala/                 thin Spark Scala job (campaign rollup)
```

---

## Domain notes (worth reading)

- Events are **SSAI-style** beacons: `ad_request`, `ad_impression`, `ad_click`, `ad_quartile`, `ad_error`.
- Pods are first-class (`pod_id`, `break_type`: preroll / midroll / postroll).
- Money fields stay integer **micros** until gold (no float CPM in silver).
- Late beacons: up to **36h** accepted into the current `dt` drop; silver merges on `event_id`.
- See [docs/DOMAIN.md](docs/DOMAIN.md) and [docs/DESIGN.md](docs/DESIGN.md).

---

## License

MIT
