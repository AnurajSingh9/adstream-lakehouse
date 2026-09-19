"""AdStream daily lakehouse.

Catchup-safe: each task keys off `ds`. Re-running a day overwrites silver for that
partition and merges gold by event_date — don't switch this to append-only without
thinking about double counts.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

REPO = Path(__file__).resolve().parents[2]


def _silver(ds: str, **_):
    from pipelines.spark.silver_ad_events import run

    run(ds)


def _gold(ds: str, **_):
    from pipelines.spark.gold_marts import run

    run(ds)


def _ge(ds: str, **_):
    from great_expectations_gate import run_gate

    run_gate(ds)


default_args = {
    "owner": "anuraj",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="adstream_daily",
    description="OTT AdTech bronze→silver→gold with DQ gate",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["adtech", "ott", "lakehouse"],
) as dag:
    # sample land is usually upstream (MSK → S3). locally we assume bronze exists.
    silver = PythonOperator(
        task_id="silver_ad_events",
        python_callable=_silver,
        op_kwargs={"ds": "{{ ds }}"},
    )
    gold = PythonOperator(
        task_id="gold_marts",
        python_callable=_gold,
        op_kwargs={"ds": "{{ ds }}"},
    )
    dbt = BashOperator(
        task_id="dbt_run_test",
        bash_command=(
            f"cd {REPO}/dbt/adstream && "
            "dbt run --profiles-dir . --project-dir . && "
            "dbt test --profiles-dir . --project-dir ."
        ),
    )
    ge = PythonOperator(
        task_id="great_expectations_gate",
        python_callable=_ge,
        op_kwargs={"ds": "{{ ds }}"},
    )

    silver >> gold >> dbt >> ge
