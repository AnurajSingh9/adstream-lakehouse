"""Fail loud if the day looks wrong.

Checks I actually care about on AdTech days:
- impressions never exceed requests by a wild margin (beacon dupes)
- fill_rate in a believable band
- revenue_micros not negative
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd

from pipelines.common.paths import GOLD, silver_dt


def run_gate(ds: str) -> None:
    parts = list(silver_dt(ds).glob("*.parquet"))
    if not parts:
        raise SystemExit(f"GE gate: no silver for dt={ds}")
    silver = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)

    n = len(silver)
    if n < 50:
        raise SystemExit(f"GE gate: silver too small ({n} rows) for dt={ds}")

    req = (silver.event_type == "ad_request").sum()
    imp = (silver.event_type == "ad_impression").sum()
    if req == 0:
        raise SystemExit("GE gate: zero ad_request rows")
    if imp > req * 1.15:
        # some late dupes ok; 15% over is usually a merge bug
        raise SystemExit(f"GE gate: impressions ({imp}) >> requests ({req})")

    camp_path = GOLD / "fct_campaign_daily.parquet"
    if camp_path.exists():
        camp = pd.read_parquet(camp_path)
        day = camp[camp["event_date"].astype(str) == ds]
        if day.empty:
            raise SystemExit(f"GE gate: no gold campaign rows for {ds}")
        if (day["revenue_micros"] < 0).any():
            raise SystemExit("GE gate: negative revenue_micros")
        fill = day["fill_rate"].dropna()
        if len(fill) and (fill.mean() < 0.2 or fill.mean() > 0.99):
            raise SystemExit(f"GE gate: suspicious mean fill_rate={fill.mean():.3f}")

    print(f"GE gate passed dt={ds} silver_rows={n} requests={req} impressions={imp}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ds", required=True)
    args = ap.parse_args()
    try:
        run_gate(args.ds)
    except SystemExit as e:
        print(e, file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
