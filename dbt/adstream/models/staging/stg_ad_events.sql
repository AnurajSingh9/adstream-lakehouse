-- Staging over silver parquet. Path is relative to duckdb file under data/duckdb/.
{{ config(materialized='view') }}

select
  event_id,
  event_type,
  cast(event_ts as timestamp) as event_ts,
  cast(ingest_ts as timestamp) as ingest_ts,
  cast(event_date as date) as event_date,
  session_id,
  content_id,
  pod_id,
  slot_index,
  break_type,
  platform,
  market,
  campaign_id,
  creative_id,
  advertiser_id,
  quartile,
  error_code,
  bid_floor_micros,
  clearing_price_micros,
  is_late
from read_parquet('../../data/silver/ad_events/dt=*/*.parquet', hive_partitioning=true)
