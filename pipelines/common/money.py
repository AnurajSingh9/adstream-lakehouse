"""Shared helpers. Money stays in micros until gold — floats in silver are how you invent money."""

from __future__ import annotations

MICROS_PER_UNIT = 1_000_000

BREAK_TYPES = ("preroll", "midroll", "postroll")
EVENT_TYPES = (
    "ad_request",
    "ad_impression",
    "ad_click",
    "ad_quartile",
    "ad_error",
)
PLATFORMS = ("android", "ios", "web", "tv")
MARKETS = ("IN", "US", "GB", "AE")


def micros_to_currency(micros: int) -> float:
    return round(micros / MICROS_PER_UNIT, 6)


def safe_rate(num: int, den: int) -> float | None:
    if den <= 0:
        return None
    return round(num / den, 6)
