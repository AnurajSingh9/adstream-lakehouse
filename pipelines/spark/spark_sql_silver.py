"""Optional Spark SQL path for the same silver contract.

Use when you've got a cluster / local Spark session. The pandas job in
`silver_ad_events.py` is the default for laptops.
"""

from __future__ import annotations

SILVER_SQL = """
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_ts DESC) AS rn
  FROM bronze_ad_events
)
SELECT
  event_id,
  event_type,
  CAST(event_ts AS TIMESTAMP) AS event_ts,
  CAST(ingest_ts AS TIMESTAMP) AS ingest_ts,
  to_date(event_ts) AS event_date,
  session_id,
  content_id,
  pod_id,
  COALESCE(slot_index, 0) AS slot_index,
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
  COALESCE(is_late, false) AS is_late
FROM ranked
WHERE rn = 1
  AND event_type IN ('ad_request','ad_impression','ad_click','ad_quartile','ad_error')
"""


def build_silver_spark(spark, bronze_path: str, silver_path: str) -> None:
    spark.read.json(bronze_path).createOrReplaceTempView("bronze_ad_events")
    spark.sql(SILVER_SQL).write.mode("overwrite").parquet(silver_path)
