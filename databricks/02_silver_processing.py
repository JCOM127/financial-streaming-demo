# Databricks notebook source
# Silver processing: parse, type, deduplicate, and enrich ticks.
#
# For the student Databricks/serverless demo, Silver is materialized as a batch
# overwrite from the Bronze Delta table. Bronze still demonstrates Auto Loader
# and Structured Streaming, while this keeps the cleaned table immediately
# queryable after each run.

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

dbutils.widgets.text("catalog", "student_streaming")
dbutils.widgets.text("schema", "market_demo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
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
    spark.table(source_table)
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

typed_df = (
    parsed
    .withColumn(
        "event_time",
        F.coalesce(
            F.to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
            F.to_timestamp(F.regexp_replace("event_time", "Z$", "+00:00")),
            F.to_timestamp("event_time"),
        ),
    )
    .withColumn(
        "producer_time",
        F.coalesce(
            F.to_timestamp("producer_time", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
            F.to_timestamp(F.regexp_replace("producer_time", "Z$", "+00:00")),
            F.to_timestamp("producer_time"),
        ),
    )
)

display(
    typed_df.select(
        F.count("*").alias("parsed_rows_before_filter"),
        F.count("event_id").alias("non_null_event_id"),
        F.count("symbol").alias("non_null_symbol"),
        F.count("event_time").alias("non_null_event_time"),
        F.count("price").alias("non_null_price"),
        F.count("volume").alias("non_null_volume"),
    )
)

display(
    typed_df.select(
        "event_id",
        "symbol",
        "event_time",
        "price",
        "volume",
        "bid",
        "ask",
        "anomaly_hint",
    ).limit(20)
)

silver_df = (
    typed_df
    .filter(
        F.col("event_id").isNotNull()
        & F.col("symbol").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("price").isNotNull()
        & F.col("volume").isNotNull()
        & (F.col("price") > 0)
        & (F.col("volume") > 0)
    )
    .dropDuplicates(["event_id"])
    .withColumn("spread", F.col("ask") - F.col("bid"))
    .withColumn("mid_price", (F.col("ask") + F.col("bid")) / F.lit(2.0))
    .withColumn("notional_value", F.col("price") * F.col("volume"))
    .withColumn("event_delay_seconds", F.col("ingestion_time").cast("long") - F.col("event_time").cast("long"))
    .withColumn("silver_processed_time", F.current_timestamp())
)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

display(
    spark.sql(
        f"""
        SELECT
          COUNT(*) AS silver_rows,
          COUNT(DISTINCT event_id) AS distinct_event_ids,
          MIN(event_time) AS earliest_event_time,
          MAX(event_time) AS latest_event_time
        FROM {target_table}
        """
    )
)
