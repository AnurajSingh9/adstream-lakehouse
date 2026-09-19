# Local setup

```bash
cd adstream-lakehouse
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=.

make sample
make silver
# pick a ds from data/silver/ad_events/
DS=$(ls data/silver/ad_events | sed 's/dt=//' | sort | tail -1)
python -m pipelines.spark.gold_marts --ds "$DS"
python great_expectations_gate.py --ds "$DS"
pytest tests/ -q
make api
```

Optional Kafka (docker):

```bash
docker compose up -d kafka
python pipelines/ingest/kafka_producer.py --file data/raw_drops/ad_events_$DS.jsonl
```

dbt (duckdb):

```bash
cd dbt/adstream
dbt run --profiles-dir . --project-dir .
dbt test --profiles-dir . --project-dir .
```

Airflow is wired for Composer/MWAA-style deploy; locally just read `airflow/dags/adstream_daily.py` — mounting the whole repo as `PYTHONPATH` is enough for unit understanding.
