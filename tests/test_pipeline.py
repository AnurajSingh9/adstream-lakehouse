from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def sample_ds():
    import os
    import subprocess
    import sys

    # fresh tiny drop so tests don't depend on whatever you last generated
    for d in ("data/bronze", "data/silver", "data/gold", "data/raw_drops"):
        p = ROOT / d
        if p.exists():
            import shutil

            shutil.rmtree(p)

    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    subprocess.check_call(
        [sys.executable, "scripts/generate_sample_data.py", "--days", "1", "--seed", "7"],
        cwd=ROOT,
        env=env,
    )
    return max(p.name.replace("dt=", "") for p in (ROOT / "data/bronze/ad_events").glob("dt=*"))


def test_schema_rejects_unknown_event():
    from jsonschema import Draft202012Validator, ValidationError

    schema = json.loads((ROOT / "schemas/ad_event.schema.json").read_text())
    v = Draft202012Validator(schema)
    bad = {
        "event_id": "x" * 8,
        "event_type": "ad_weird",
        "event_ts": "2026-09-18T10:00:00+00:00",
        "session_id": "s",
        "content_id": "c",
        "pod_id": "p",
        "break_type": "midroll",
        "platform": "android",
        "market": "IN",
        "campaign_id": "cmp",
    }
    with pytest.raises(ValidationError):
        v.validate(bad)


def test_silver_dedupes(sample_ds):
    from pipelines.spark.silver_ad_events import run

    path = run(sample_ds)
    df = pd.read_parquet(path)
    assert df["event_id"].is_unique
    assert set(df["event_type"]) <= {
        "ad_request",
        "ad_impression",
        "ad_click",
        "ad_quartile",
        "ad_error",
    }


def test_gold_metrics(sample_ds):
    from pipelines.spark.gold_marts import run
    from pipelines.spark.silver_ad_events import run as silver_run

    silver_run(sample_ds)
    run(sample_ds)
    camp = pd.read_parquet(ROOT / "data/gold/fct_campaign_daily.parquet")
    day = camp[camp["event_date"].astype(str) == sample_ds]
    assert not day.empty
    assert day["revenue_micros"].min() >= 0
    assert day["requests"].sum() > 0
    assert day["impressions"].sum() > 0
    # fill rate defined where requests > 0
    filled = day[day["requests"] > 0]["fill_rate"].dropna()
    assert (filled >= 0).all()
    # late impressions can push a grain slightly over 1.0 — same thing GE allows
    assert (filled <= 1.15).all()


def test_ge_gate(sample_ds):
    from great_expectations_gate import run_gate
    from pipelines.spark.gold_marts import run as gold_run
    from pipelines.spark.silver_ad_events import run as silver_run

    silver_run(sample_ds)
    gold_run(sample_ds)
    run_gate(sample_ds)
