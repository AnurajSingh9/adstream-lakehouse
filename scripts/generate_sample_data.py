"""Synthetic SSAI beacons.

Not uniform random noise — midrolls fill worse than prerolls, TV has fewer clicks,
and ~2% of impressions show up late (the thing that breaks naive daily jobs).
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone

from pipelines.common.money import BREAK_TYPES, MARKETS, PLATFORMS
from pipelines.common.paths import ROOT

CAMPAIGNS = [
    ("cmp_cricket_peak", "adv_paytm", "cr_bat_15s"),
    ("cmp_ipl_bumper", "adv_flipkart", "cr_cart_30s"),
    ("cmp_show_launch", "adv_disney", "cr_trailer_20s"),
    ("cmp_evergreen_roa", "adv_swiggy", "cr_food_15s"),
    ("cmp_brand_lift", "adv_hdfc", "cr_card_15s"),
]
CONTENTS = [f"cnt_{i:04d}" for i in range(1, 41)]


def _ts(day: datetime, hour: int, minute: int, second: int = 0) -> str:
    return day.replace(hour=hour, minute=minute, second=second, microsecond=0).isoformat()


def _event(
    *,
    event_type: str,
    event_ts: str,
    session_id: str,
    content_id: str,
    pod_id: str,
    slot_index: int,
    break_type: str,
    platform: str,
    market: str,
    campaign_id: str,
    creative_id: str | None,
    advertiser_id: str | None,
    quartile: int | None = None,
    error_code: str | None = None,
    bid_floor_micros: int | None = None,
    clearing_price_micros: int | None = None,
    is_late: bool = False,
) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": event_ts,
        "ingest_ts": event_ts,
        "session_id": session_id,
        "content_id": content_id,
        "pod_id": pod_id,
        "slot_index": slot_index,
        "break_type": break_type,
        "platform": platform,
        "market": market,
        "campaign_id": campaign_id,
        "creative_id": creative_id,
        "advertiser_id": advertiser_id,
        "quartile": quartile,
        "error_code": error_code,
        "bid_floor_micros": bid_floor_micros,
        "clearing_price_micros": clearing_price_micros,
        "is_late": is_late,
    }


def generate_day(day: datetime, rng: random.Random, sessions: int = 180) -> list[dict]:
    rows: list[dict] = []
    # evening skew — OTT traffic isn't flat
    hour_weights = [1, 1, 1, 1, 2, 3, 4, 6, 8, 9, 7, 5, 4, 3, 3, 4, 6, 9, 12, 14, 13, 10, 6, 3]

    for _ in range(sessions):
        hour = rng.choices(range(24), weights=hour_weights, k=1)[0]
        minute = rng.randint(0, 59)
        platform = rng.choice(PLATFORMS)
        market = rng.choices(MARKETS, weights=[70, 12, 10, 8], k=1)[0]
        content_id = rng.choice(CONTENTS)
        session_id = f"sess_{uuid.uuid4().hex[:12]}"
        break_type = rng.choices(BREAK_TYPES, weights=[25, 60, 15], k=1)[0]
        pod_id = f"pod_{uuid.uuid4().hex[:10]}"
        campaign_id, advertiser_id, creative_id = rng.choice(CAMPAIGNS)
        slots = 1 if break_type == "preroll" else rng.choice([1, 2, 3])

        base = day.replace(tzinfo=timezone.utc)
        for slot in range(slots):
            # fill probability: preroll healthier than midroll
            fill_p = {"preroll": 0.92, "midroll": 0.78, "postroll": 0.85}[break_type]
            if platform == "tv":
                fill_p -= 0.04

            bid_floor = rng.randint(80_000, 450_000)  # micros
            req_ts = _ts(base, hour, minute, rng.randint(0, 40))
            rows.append(
                _event(
                    event_type="ad_request",
                    event_ts=req_ts,
                    session_id=session_id,
                    content_id=content_id,
                    pod_id=pod_id,
                    slot_index=slot,
                    break_type=break_type,
                    platform=platform,
                    market=market,
                    campaign_id=campaign_id,
                    creative_id=None,
                    advertiser_id=None,
                    bid_floor_micros=bid_floor,
                )
            )

            if rng.random() > fill_p:
                rows.append(
                    _event(
                        event_type="ad_error",
                        event_ts=_ts(base, hour, minute, min(59, rng.randint(1, 55))),
                        session_id=session_id,
                        content_id=content_id,
                        pod_id=pod_id,
                        slot_index=slot,
                        break_type=break_type,
                        platform=platform,
                        market=market,
                        campaign_id=campaign_id,
                        creative_id=None,
                        advertiser_id=None,
                        error_code=rng.choice(["NO_AD", "TIMEOUT", "VAST_ERROR"]),
                    )
                )
                continue

            clear = int(bid_floor * rng.uniform(0.9, 1.35))
            imp_ts = _ts(base, hour, minute, min(59, rng.randint(2, 56)))
            rows.append(
                _event(
                    event_type="ad_impression",
                    event_ts=imp_ts,
                    session_id=session_id,
                    content_id=content_id,
                    pod_id=pod_id,
                    slot_index=slot,
                    break_type=break_type,
                    platform=platform,
                    market=market,
                    campaign_id=campaign_id,
                    creative_id=creative_id,
                    advertiser_id=advertiser_id,
                    clearing_price_micros=clear,
                )
            )

            # late *retry* of the same impression — same event_id, later ingest
            if rng.random() < 0.02:
                late = dict(rows[-1])
                late["is_late"] = True
                late_dt = datetime.fromisoformat(imp_ts) + timedelta(hours=rng.randint(6, 30))
                late["ingest_ts"] = late_dt.isoformat()
                # event_ts stays; ingest moves — silver keeps last ingest_ts
                rows.append(late)

            if platform != "tv" and rng.random() < 0.045:
                rows.append(
                    _event(
                        event_type="ad_click",
                        event_ts=_ts(base, hour, minute, min(59, rng.randint(3, 58))),
                        session_id=session_id,
                        content_id=content_id,
                        pod_id=pod_id,
                        slot_index=slot,
                        break_type=break_type,
                        platform=platform,
                        market=market,
                        campaign_id=campaign_id,
                        creative_id=creative_id,
                        advertiser_id=advertiser_id,
                    )
                )

            # quartile ladder — not everyone finishes
            q_roll = rng.random()
            for q, thresh in ((25, 0.0), (50, 0.18), (75, 0.35), (100, 0.48)):
                if q_roll < thresh:
                    break
                rows.append(
                    _event(
                        event_type="ad_quartile",
                        event_ts=_ts(base, hour, minute, min(59, rng.randint(4, 59))),
                        session_id=session_id,
                        content_id=content_id,
                        pod_id=pod_id,
                        slot_index=slot,
                        break_type=break_type,
                        platform=platform,
                        market=market,
                        campaign_id=campaign_id,
                        creative_id=creative_id,
                        advertiser_id=advertiser_id,
                        quartile=q,
                    )
                )

    rng.shuffle(rows)
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--start", default=None, help="YYYY-MM-DD (default: 3 days ago UTC)")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    if args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
            days=args.days
        )

    raw_dir = ROOT / "data" / "raw_drops"
    raw_dir.mkdir(parents=True, exist_ok=True)

    from pipelines.ingest.land_events import land_file

    for i in range(args.days):
        day = start + timedelta(days=i)
        ds = day.strftime("%Y-%m-%d")
        rows = generate_day(day, rng)
        drop = raw_dir / f"ad_events_{ds}.jsonl"
        with drop.open("w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
        ok, bad = land_file(drop, ds)
        print(f"{ds}: generated={len(rows)} landed={ok} quarantined={bad}")


if __name__ == "__main__":
    main()
