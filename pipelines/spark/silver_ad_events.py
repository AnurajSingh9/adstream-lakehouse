"""Bronze JSONL → silver Parquet.

Idempotent on event_id within a dt partition. Prefer Spark when available;
falls back to pandas so `make silver` works on a laptop without a cluster.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from pipelines.common.paths import bronze_dt, silver_dt

REQUIRED = [
    "event_id",
    "event_type",
    "event_ts",
    "session_id",
    "content_id",
    "pod_id",
    "break_type",
    "platform",
    "market",
    "campaign_id",
]


def _read_bronze(ds: str) -> pd.DataFrame:
    folder = bronze_dt(ds)
    if not folder.exists():
        raise FileNotFoundError(f"no bronze for dt={ds}")
    rows: list[dict] = []
    for part in sorted(folder.glob("*.jsonl")):
        with part.open() as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    if not rows:
        raise ValueError(f"empty bronze dt={ds}")
    return pd.DataFrame(rows)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"silver missing columns: {missing}")

    out = df.copy()
    out["event_ts"] = pd.to_datetime(out["event_ts"], utc=True)
    out["ingest_ts"] = pd.to_datetime(out.get("ingest_ts", out["event_ts"]), utc=True)
    out["event_date"] = out["event_ts"].dt.strftime("%Y-%m-%d")
    out["slot_index"] = out.get("slot_index", 0).fillna(0).astype(int)
    out["is_late"] = out.get("is_late", False).fillna(False).astype(bool)

    for col in ("bid_floor_micros", "clearing_price_micros", "quartile"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    # last write wins for duplicate event_id (late repairs / retries)
    out = out.sort_values(["event_id", "ingest_ts"]).drop_duplicates("event_id", keep="last")

    # drop obvious junk — request without campaign already blocked by schema,
    # but impressions with null creative are still useful for fill math
    out = out[out["event_type"].isin(
        ["ad_request", "ad_impression", "ad_click", "ad_quartile", "ad_error"]
    )]
    return out.reset_index(drop=True)


def write_silver(df: pd.DataFrame, ds: str) -> Path:
    out_dir = silver_dt(ds)
    if out_dir.exists():
        for p in out_dir.glob("*"):
            if p.is_file():
                p.unlink()
    else:
        out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "part-000.parquet"
    df.to_parquet(path, index=False)
    return path


def run(ds: str) -> Path:
    bronze = _read_bronze(ds)
    silver = transform(bronze)
    path = write_silver(silver, ds)
    print(
        f"silver dt={ds} rows={len(silver)} "
        f"impressions={(silver.event_type == 'ad_impression').sum()} → {path}"
    )
    return path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ds", required=True)
    args = ap.parse_args()
    run(args.ds)


if __name__ == "__main__":
    main()
