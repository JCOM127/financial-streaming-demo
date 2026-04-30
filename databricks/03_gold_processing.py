# Databricks notebook source
# Gold processing: dashboard-ready KPIs, anomalies, and market health.
#
# Bronze and Silver demonstrate Structured Streaming. Gold is intentionally
# materialized as a batch overwrite over the Silver Delta table so the AI/BI
# dashboard is populated immediately after each scheduled AvailableNow run.

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "student_streaming")
dbutils.widgets.text("schema", "market_demo")
dbutils.widgets.text("checkpoint_root", "/Volumes/student_streaming/market_demo/checkpoints")
dbutils.widgets.text("volume_spike_threshold", "10000")
dbutils.widgets.text("spread_pct_threshold", "0.02")
dbutils.widgets.text("delay_threshold_seconds", "60")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
checkpoint_root = dbutils.widgets.get("checkpoint_root")
source_table = f"{catalog}.{schema}.silver_ticks_clean"
kpi_table = f"{catalog}.{schema}.gold_symbol_kpis"
anomaly_table = f"{catalog}.{schema}.gold_anomalies"
health_table = f"{catalog}.{schema}.gold_market_health"

volume_spike_threshold = float(dbutils.widgets.get("volume_spike_threshold"))
spread_pct_threshold = float(dbutils.widgets.get("spread_pct_threshold"))
delay_threshold_seconds = float(dbutils.widgets.get("delay_threshold_seconds"))

silver_static = spark.table(source_table)

kpis = (
    silver_static
    .groupBy(F.window("event_time", "1 minute"), "symbol")
    .agg(
        F.count("*").alias("trade_count"),
        F.sum("volume").alias("total_volume"),
        F.avg("price").alias("avg_price"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        (F.sum(F.col("price") * F.col("volume")) / F.sum("volume")).alias("vwap"),
        F.avg("spread").alias("avg_spread"),
        F.max("event_delay_seconds").alias("max_event_delay_seconds"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "symbol",
        "trade_count",
        "total_volume",
        "avg_price",
        "min_price",
        "max_price",
        "vwap",
        "avg_spread",
        "max_event_delay_seconds",
        F.current_timestamp().alias("gold_processed_time"),
    )
)

(
    kpis.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(kpi_table)
)

volume_anomalies = silver_static.filter(F.col("volume") >= volume_spike_threshold).select(
    "event_id",
    "symbol",
    "event_time",
    F.lit("volume_spike").alias("anomaly_type"),
    F.col("volume").cast("double").alias("observed_value"),
    F.lit(volume_spike_threshold).alias("threshold"),
    F.lit("high").alias("severity"),
    "anomaly_hint",
)

spread_anomalies = silver_static.filter((F.col("spread") / F.col("price")) >= spread_pct_threshold).select(
    "event_id",
    "symbol",
    "event_time",
    F.lit("wide_spread").alias("anomaly_type"),
    (F.col("spread") / F.col("price")).alias("observed_value"),
    F.lit(spread_pct_threshold).alias("threshold"),
    F.lit("medium").alias("severity"),
    "anomaly_hint",
)

delay_anomalies = silver_static.filter(F.col("event_delay_seconds") >= delay_threshold_seconds).select(
    "event_id",
    "symbol",
    "event_time",
    F.lit("delayed_event").alias("anomaly_type"),
    F.col("event_delay_seconds").cast("double").alias("observed_value"),
    F.lit(delay_threshold_seconds).alias("threshold"),
    F.lit("medium").alias("severity"),
    "anomaly_hint",
)

price_hint_anomalies = silver_static.filter(F.col("anomaly_hint") == "price_jump").select(
    "event_id",
    "symbol",
    "event_time",
    F.lit("price_jump").alias("anomaly_type"),
    F.col("price").cast("double").alias("observed_value"),
    F.lit(None).cast("double").alias("threshold"),
    F.lit("high").alias("severity"),
    "anomaly_hint",
)

(
    volume_anomalies
    .unionByName(spread_anomalies)
    .unionByName(delay_anomalies)
    .unionByName(price_hint_anomalies)
    .dropDuplicates(["event_id", "anomaly_type"])
    .withColumn("gold_processed_time", F.current_timestamp())
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(anomaly_table)
)

latest_by_symbol = (
    silver_static
    .groupBy("symbol")
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.max_by("price", "event_time").alias("last_price"),
        F.count("*").alias("total_events"),
        F.sum("volume").alias("total_volume"),
        F.max("event_delay_seconds").alias("max_event_delay_seconds"),
    )
)

anomalies_by_symbol = spark.table(anomaly_table).groupBy("symbol").agg(F.count("*").alias("total_anomalies"))

(
    latest_by_symbol
    .join(anomalies_by_symbol, on="symbol", how="left")
    .fillna({"total_anomalies": 0})
    .withColumn("health_checked_time", F.current_timestamp())
    .withColumn(
        "freshness_status",
        F.when(
            F.col("latest_event_time") < F.current_timestamp() - F.expr("INTERVAL 5 MINUTES"),
            F.lit("STALE"),
        ).otherwise(F.lit("FRESH")),
    )
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(health_table)
)
