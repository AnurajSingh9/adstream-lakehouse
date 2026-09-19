# ADR 0001 — Micros for money, not floats

## Status

Accepted

## Context

Ad clearing prices look tiny (fractions of a rupee/dollar). Storing them as float64 in silver is how you invent or lose cents across billions of impressions.

## Decision

Keep `bid_floor_micros` and `clearing_price_micros` as integers through bronze and silver. Convert in gold / API only.

## Consequences

- dbt `revenue` is derived: `revenue_micros / 1e6`
- eCPM computed after conversion
- Slightly more verbose code; finance stops pinging you
