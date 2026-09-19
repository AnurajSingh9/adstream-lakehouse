"""Build gold parquet marts from silver — used when dbt isn't wired yet / CI smoke."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from pipelines.common.money import micros_to_currency, safe_rate
from pipelines.common.paths import GOLD, silver_dt


def load_silver(ds: str) -> pd.DataFrame:
    parts = list(silver_dt(ds).glob("*.parquet"))
    if not parts:
        raise FileNotFoundError(silver_dt(ds))
    return pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)


def campaign_daily(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["event_date"] = df["event_date"].astype(str)
    keys = ["event_date", "campaign_id", "market", "platform", "break_type"]

    req = df[df.event_type == "ad_request"].groupby(keys).size().rename("requests")
    imp = df[df.event_type == "ad_impression"].groupby(keys).size().rename("impressions")
    clk = df[df.event_type == "ad_click"].groupby(keys).size().rename("clicks")
    err = df[df.event_type == "ad_error"].groupby(keys).size().rename("errors")
    rev = (
        df[df.event_type == "ad_impression"]
        .groupby(keys)["clearing_price_micros"]
        .sum()
        .rename("revenue_micros")
    )
    q100 = (
        df[(df.event_type == "ad_quartile") & (df.quartile == 100)]
        .groupby(keys)
        .size()
        .rename("completions")
    )

    out = pd.concat([req, imp, clk, err, rev, q100], axis=1).fillna(0).reset_index()
    for c in ["requests", "impressions", "clicks", "errors", "completions"]:
        out[c] = out[c].astype(int)
    out["revenue_micros"] = out["revenue_micros"].astype(int)
    out["fill_rate"] = out.apply(lambda r: safe_rate(r.impressions, r.requests), axis=1)
    out["ctr"] = out.apply(lambda r: safe_rate(r.clicks, r.impressions), axis=1)
    out["completion_rate"] = out.apply(lambda r: safe_rate(r.completions, r.impressions), axis=1)
    out["revenue"] = out["revenue_micros"].map(micros_to_currency)
    # eCPM = revenue / impressions * 1000
    out["ecpm"] = out.apply(
        lambda r: round((r.revenue / r.impressions) * 1000, 4) if r.impressions else None,
        axis=1,
    )
    return out


def pod_fill(df: pd.DataFrame) -> pd.DataFrame:
    """Pod-level fill — useful when midroll pods request 3 and fill 1."""
    g = df.groupby(["event_date", "pod_id", "break_type", "content_id", "platform", "market"], dropna=False)
    rows = []
    for keys, part in g:
        event_date, pod_id, break_type, content_id, platform, market = keys
        rows.append(
            {
                "event_date": event_date,
                "pod_id": pod_id,
                "break_type": break_type,
                "content_id": content_id,
                "platform": platform,
                "market": market,
                "requests": int((part.event_type == "ad_request").sum()),
                "impressions": int((part.event_type == "ad_impression").sum()),
                "errors": int((part.event_type == "ad_error").sum()),
                "filled": int((part.event_type == "ad_impression").sum() > 0),
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["pod_fill_rate"] = out.apply(lambda r: safe_rate(r.impressions, r.requests), axis=1)
    return out


def run(ds: str) -> None:
    silver = load_silver(ds)
    GOLD.mkdir(parents=True, exist_ok=True)
    c = campaign_daily(silver)
    p = pod_fill(silver)
    c_path = GOLD / "fct_campaign_daily.parquet"
    p_path = GOLD / "fct_pod_fill.parquet"
    # append-by-date overwrite semantics
    for path, frame, key in (
        (c_path, c, "event_date"),
        (p_path, p, "event_date"),
    ):
        if path.exists():
            old = pd.read_parquet(path)
            old = old[old[key].astype(str) != ds]
            frame = pd.concat([old, frame], ignore_index=True)
        frame.to_parquet(path, index=False)
        print(f"wrote {path.name} rows={len(frame)} (ds={ds})")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ds", required=True)
    args = ap.parse_args()
    run(args.ds)


if __name__ == "__main__":
    main()
