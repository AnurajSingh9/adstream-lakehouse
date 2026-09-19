"""Lake paths. Local layout mirrors what we'd put under s3://adstream-prod/ or gs://adstream-prod/."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"

BRONZE = DATA / "bronze" / "ad_events"
SILVER = DATA / "silver" / "ad_events"
GOLD = DATA / "gold"
QUARANTINE = DATA / "bronze" / "_quarantine"


def bronze_dt(ds: str) -> Path:
    return BRONZE / f"dt={ds}"


def silver_dt(ds: str) -> Path:
    return SILVER / f"dt={ds}"


def gold_table(name: str) -> Path:
    return GOLD / name
