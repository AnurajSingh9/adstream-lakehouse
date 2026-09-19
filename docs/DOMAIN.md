# Domain glossary — OTT / SSAI AdTech as used in this repo

## Events

| event_type | Meaning |
|------------|---------|
| `ad_request` | Ad decision / VAST request for a pod slot |
| `ad_impression` | Creative started (billable in most contracts) |
| `ad_click` | User click-through |
| `ad_quartile` | 25 / 50 / 75 / 100 view progress |
| `ad_error` | No-fill, timeout, VAST parse, etc. |

## Pods & breaks

- **pod** — a commercial break inside a stream (`pod_id`)
- **preroll** — before content; usually highest fill
- **midroll** — during content; more slots, worse fill on sparse invent
- **postroll** — after content; noisy, often ignored by product

## Money

- All auction prices stored as **integer micros** (`clearing_price_micros`)
- Convert to currency only in gold / API
- **eCPM** = revenue / impressions * 1000

## Metrics we expose

- **fill_rate** = impressions / requests
- **ctr** = clicks / impressions
- **completion_rate** = quartile-100 / impressions
- **pod_fill_rate** = same math at pod grain (shows underfilled midrolls)

## Why late beacons matter

SSAI clients buffer and retry. A naive `dt=event_date` job that only reads “today’s” files will undercount completions and disagree with finance. We accept late arrivals into bronze and merge silver on `event_id`.
