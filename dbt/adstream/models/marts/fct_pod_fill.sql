{{ config(materialized='table') }}

with base as (
  select
    event_date,
    pod_id,
    break_type,
    content_id,
    platform,
    market,
    count_if(event_type = 'ad_request') as requests,
    count_if(event_type = 'ad_impression') as impressions,
    count_if(event_type = 'ad_error') as errors
  from {{ ref('stg_ad_events') }}
  group by 1, 2, 3, 4, 5, 6
)

select
  *,
  case when impressions > 0 then 1 else 0 end as filled,
  case when requests > 0 then round(impressions * 1.0 / requests, 6) end as pod_fill_rate
from base
