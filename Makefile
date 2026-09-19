.PHONY: sample silver gold ge test api lint fmt

export PYTHONPATH := .

sample:
	python scripts/generate_sample_data.py --days 3 --seed 42

silver:
	@DS=$$(ls data/bronze/ad_events | sed 's/dt=//' | sort | tail -1); \
	python -m pipelines.spark.silver_ad_events --ds $$DS

gold:
	@DS=$$(ls data/silver/ad_events | sed 's/dt=//' | sort | tail -1); \
	python -m pipelines.spark.gold_marts --ds $$DS

dbt-run:
	cd dbt/adstream && dbt run --profiles-dir . --project-dir . && dbt test --profiles-dir . --project-dir .

ge:
	@DS=$$(ls data/silver/ad_events | sed 's/dt=//' | sort | tail -1); \
	python great_expectations_gate.py --ds $$DS

test:
	pytest tests/ -q

api:
	uvicorn api.app:app --reload --port 8088

lint:
	ruff check pipelines scripts api tests great_expectations_gate.py

fmt:
	ruff format pipelines scripts api tests great_expectations_gate.py
