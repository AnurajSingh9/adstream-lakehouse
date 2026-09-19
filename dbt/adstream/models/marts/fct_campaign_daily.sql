{{ config(materialized='table') }}

select
  event_date,
  campaign_id,
  market,
  platform,
  break_type,
  requests,
  impressions,
  clicks,
  errors,
  completions,
  revenue_micros,
  round(revenue_micros / 1000000.0, 6) as revenue,
  case when requests > 0 then round(impressions * 1.0 / requests, 6) end as fill_rate,
  case when impressions > 0 then round(clicks * 1.0 / impressions, 6) end as ctr,
  case when impressions > 0 then round(completions * 1.0 / impressions, 6) end as completion_rate,
  case
    when impressions > 0
    then round((revenue_micros / 1000000.0) / impressions * 1000, 4)
  end as ecpm
from {{ ref('int_campaign_metrics') }}
