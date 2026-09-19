{{ config(materialized='view') }}

select
  event_date,
  campaign_id,
  market,
  platform,
  break_type,
  count_if(event_type = 'ad_request') as requests,
  count_if(event_type = 'ad_impression') as impressions,
  count_if(event_type = 'ad_click') as clicks,
  count_if(event_type = 'ad_error') as errors,
  count_if(event_type = 'ad_quartile' and quartile = 100) as completions,
  coalesce(sum(case when event_type = 'ad_impression' then clearing_price_micros else 0 end), 0)
    as revenue_micros
from {{ ref('stg_ad_events') }}
group by 1, 2, 3, 4, 5
