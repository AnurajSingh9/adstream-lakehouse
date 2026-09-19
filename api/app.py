"""Read API over gold marts. Enough for a demo / portfolio walkthrough."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

from pipelines.common.paths import GOLD

app = FastAPI(
    title="AdStream API",
    description="OTT AdTech gold marts — campaign daily + pod fill",
    version="0.1.0",
)


def _read(name: str) -> pd.DataFrame:
    path = GOLD / name
    if not path.exists():
        raise HTTPException(503, f"{name} not built yet — run make sample && make silver && python -m pipelines.spark.gold_marts")
    return pd.read_parquet(path)


@app.get("/health")
def health():
    return {"ok": True, "gold": GOLD.exists()}


@app.get("/v1/campaigns/daily")
def campaigns_daily(
    ds: str | None = Query(None, description="YYYY-MM-DD"),
    campaign_id: str | None = None,
    market: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
):
    df = _read("fct_campaign_daily.parquet")
    if ds:
        df = df[df["event_date"].astype(str) == ds]
    if campaign_id:
        df = df[df["campaign_id"] == campaign_id]
    if market:
        df = df[df["market"] == market]
    df = df.sort_values(["event_date", "revenue_micros"], ascending=[False, False]).head(limit)
    return {"count": len(df), "rows": df.to_dict(orient="records")}


@app.get("/v1/pods/fill")
def pods_fill(
    ds: str | None = None,
    break_type: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
):
    df = _read("fct_pod_fill.parquet")
    if ds:
        df = df[df["event_date"].astype(str) == ds]
    if break_type:
        df = df[df["break_type"] == break_type]
    df = df.sort_values(["event_date", "requests"], ascending=[False, False]).head(limit)
    return {"count": len(df), "rows": df.to_dict(orient="records")}


@app.get("/v1/summary/{ds}")
def day_summary(ds: str):
    df = _read("fct_campaign_daily.parquet")
    day = df[df["event_date"].astype(str) == ds]
    if day.empty:
        raise HTTPException(404, f"no data for {ds}")
    return {
        "ds": ds,
        "requests": int(day["requests"].sum()),
        "impressions": int(day["impressions"].sum()),
        "clicks": int(day["clicks"].sum()),
        "revenue": float(day["revenue"].sum()),
        "fill_rate": round(day["impressions"].sum() / max(day["requests"].sum(), 1), 4),
        "campaigns": int(day["campaign_id"].nunique()),
    }
