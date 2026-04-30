# Databricks notebook source
# Silver processing: parse, type, deduplicate, watermark, and enrich ticks.

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

dbutils.widgets.text("catalog", "student_streaming")
dbutils.widgets.text("schema", "market_demo")
dbutils.widgets.text("checkpoint_path", "/Volumes/student_streaming/market_demo/checkpoints/silver_ticks_clean")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
source_table = f"{catalog}.{schema}.bronze_ticks_raw"
target_table = f"{catalog}.{schema}.silver_ticks_clean"

tick_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("volume", LongType(), False),
        StructField("bid", DoubleType(), True),
        StructField("ask", DoubleType(), True),
        StructField("exchange", StringType(), True),
        StructField("sequence_number", LongType(), True),
        StructField("producer_time", StringType(), True),
        StructField("anomaly_hint", StringType(), True),
    ]
)

parsed = (
    spark.readStream.table(source_table)
    .select(
        F.from_json("raw_json", tick_schema).alias("tick"),
        "ingestion_time",
        "source_type",
        "topic",
        "partition",
        "offset",
    )
    .select("tick.*", "ingestion_time", "source_type", "topic", "partition", "offset")
)

silver_df = (
    parsed
    .withColumn("event_time", F.to_timestamp("event_time"))
    .withColumn("producer_time", F.to_timestamp("producer_time"))
    .filter(
        F.col("event_id").isNotNull()
        & F.col("symbol").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("price").isNotNull()
        & F.col("volume").isNotNull()
        & (F.col("price") > 0)
        & (F.col("volume") > 0)
    )
    .withWatermark("event_time", "2 minutes")
    .dropDuplicates(["event_id"])
    .withColumn("spread", F.col("ask") - F.col("bid"))
    .withColumn("mid_price", (F.col("ask") + F.col("bid")) / F.lit(2.0))
    .withColumn("notional_value", F.col("price") * F.col("volume"))
    .withColumn("event_delay_seconds", F.col("ingestion_time").cast("long") - F.col("event_time").cast("long"))
    .withColumn("silver_processed_time", F.current_timestamp())
)

(
    silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(target_table)
)

