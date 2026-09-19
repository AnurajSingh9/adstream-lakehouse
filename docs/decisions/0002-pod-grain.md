# ADR 0002 — Pod grain in gold

## Status

Accepted

## Context

Campaign daily is what leadership wants. Ops / ads delivery cares whether a **midroll pod** requested three slots and filled one — that signal disappears if you only aggregate by campaign.

## Decision

Ship `fct_pod_fill` next to `fct_campaign_daily`.

## Consequences

- More rows in gold
- Explains fill-rate dips when content mix shifts toward long-form midrolls
