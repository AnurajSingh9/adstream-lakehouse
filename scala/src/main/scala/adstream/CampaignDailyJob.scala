// Thin Spark Scala job — same gold grain as the Python mart.
// Intentionally small: shows you can own the JVM path without a second product.

package adstream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CampaignDailyJob {
  def main(args: Array[String]): Unit = {
    val ds = args.toList match {
      case d :: _ => d
      case _ => throw new IllegalArgumentException("usage: CampaignDailyJob <ds>")
    }

    val spark = SparkSession.builder
      .appName(s"adstream-campaign-daily-$ds")
      .getOrCreate()

    import spark.implicits._

    val silver = spark.read.parquet(s"data/silver/ad_events/dt=$ds")

    val out = silver
      .groupBy($"event_date", $"campaign_id", $"market", $"platform", $"break_type")
      .agg(
        sum(when($"event_type" === "ad_request", 1).otherwise(0)).as("requests"),
        sum(when($"event_type" === "ad_impression", 1).otherwise(0)).as("impressions"),
        sum(when($"event_type" === "ad_click", 1).otherwise(0)).as("clicks"),
        sum(
          when($"event_type" === "ad_impression", $"clearing_price_micros").otherwise(0)
        ).as("revenue_micros")
      )
      .withColumn(
        "fill_rate",
        when($"requests" > 0, $"impressions" / $"requests")
      )
      .withColumn(
        "ecpm",
        when(
          $"impressions" > 0,
          ($"revenue_micros" / lit(1000000.0)) / $"impressions" * lit(1000.0)
        )
      )

    out.write.mode("overwrite").parquet(s"data/gold/scala_fct_campaign_daily/dt=$ds")
    spark.stop()
  }
}
