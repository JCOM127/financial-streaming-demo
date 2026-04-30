# Databricks notebook source
# Bronze ingestion for Kafka/Event Hub or Auto Loader file fallback.

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "student_streaming")
dbutils.widgets.text("schema", "market_demo")
dbutils.widgets.dropdown("source_mode", "files", ["files", "kafka"])
dbutils.widgets.text("raw_path", "/Volumes/student_streaming/market_demo/raw_ticks")
dbutils.widgets.text("checkpoint_path", "/Volumes/student_streaming/market_demo/checkpoints/bronze_ticks_raw")
dbutils.widgets.text("schema_path", "/Volumes/student_streaming/market_demo/schemas/bronze_ticks_raw")
dbutils.widgets.text("kafka_bootstrap_servers", "")
dbutils.widgets.text("kafka_topic", "financial_ticks")
dbutils.widgets.text("kafka_security_protocol", "SASL_SSL")
dbutils.widgets.text("kafka_sasl_mechanism", "PLAIN")
dbutils.widgets.text("kafka_username_secret_scope", "")
dbutils.widgets.text("kafka_username_secret_key", "")
dbutils.widgets.text("kafka_password_secret_scope", "")
dbutils.widgets.text("kafka_password_secret_key", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_mode = dbutils.widgets.get("source_mode")
target_table = f"{catalog}.{schema}.bronze_ticks_raw"
checkpoint_path = dbutils.widgets.get("checkpoint_path")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def secret_or_blank(scope_widget: str, key_widget: str) -> str:
    scope = dbutils.widgets.get(scope_widget)
    key = dbutils.widgets.get(key_widget)
    if not scope or not key:
        return ""
    return dbutils.secrets.get(scope=scope, key=key)


if source_mode == "kafka":
    kafka_username = secret_or_blank("kafka_username_secret_scope", "kafka_username_secret_key")
    kafka_password = secret_or_blank("kafka_password_secret_scope", "kafka_password_secret_key")

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", dbutils.widgets.get("kafka_bootstrap_servers"))
        .option("subscribe", dbutils.widgets.get("kafka_topic"))
        .option("startingOffsets", "earliest")
    )

    if kafka_username and kafka_password:
        reader = (
            reader.option("kafka.security.protocol", dbutils.widgets.get("kafka_security_protocol"))
            .option("kafka.sasl.mechanism", dbutils.widgets.get("kafka_sasl_mechanism"))
            .option("kafka.sasl.jaas.config", f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";')
        )

    bronze_df = (
        reader.load()
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("source_timestamp"),
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("raw_json"),
            F.current_timestamp().alias("ingestion_time"),
            F.lit("kafka").alias("source_type"),
        )
    )
else:
    raw_path = dbutils.widgets.get("raw_path")
    schema_path = dbutils.widgets.get("schema_path")
    bronze_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .load(raw_path)
        .select(
            F.lit(None).cast("string").alias("topic"),
            F.lit(None).cast("int").alias("partition"),
            F.lit(None).cast("long").alias("offset"),
            F.col("_metadata.file_modification_time").alias("source_timestamp"),
            F.col("symbol").cast("string").alias("message_key"),
            F.to_json(F.struct("*")).alias("raw_json"),
            F.current_timestamp().alias("ingestion_time"),
            F.lit("files").alias("source_type"),
        )
    )

(
    bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(target_table)
)

