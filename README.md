# Databricks Financial Tick Streaming Demo

Student-friendly streaming lakehouse demo for Databricks Free Edition.

The project simulates live financial ticks, ingests them through Kafka/Event Hub
or incremental JSON files, processes them through Bronze/Silver/Gold Delta
tables, and exposes dashboard-ready Gold tables for Databricks AI/BI.

## Project Layout

```text
financial-streaming-demo/
  producer/                  # Local Python tick producer
  databricks/                # Databricks notebook-compatible pipeline scripts
  configs/                   # Example local and Databricks configuration
  docs/                      # Setup notes and demo script
  tests/                     # Lightweight producer tests
  data/raw_ticks/            # Local file-mode output
```

## Quick Start: Local File Producer

File mode has no third-party runtime dependency.

```bash
cd financial-streaming-demo
python3 -m producer.market_stream.cli \
  --mode files \
  --config configs/local.example.yaml \
  --events-per-second 5 \
  --duration-seconds 30 \
  --inject-anomalies
```

The producer writes newline-delimited JSON files into `data/raw_ticks/`.
Upload or sync these files to a Databricks Unity Catalog volume such as:

```text
/Volumes/student_streaming/market_demo/raw_ticks/
```

## Kafka/Event Hub Mode

Install the optional Kafka dependency and create a private config file:

```bash
python3 -m pip install -r requirements.txt
cp configs/local.example.yaml configs/local.yaml
```

Edit `configs/local.yaml` with broker details, or set credentials with
environment variables:

```bash
export KAFKA_USERNAME="..."
export KAFKA_PASSWORD="..."
```

Run:

```bash
python3 -m producer.market_stream.cli \
  --mode kafka \
  --config configs/local.yaml \
  --events-per-second 5 \
  --duration-seconds 60 \
  --inject-anomalies
```

## Databricks Flow

Run these files in Databricks in order:

1. `databricks/00_setup.sql`
2. `databricks/01_bronze_ingestion.py`
3. `databricks/02_silver_processing.py`
4. `databricks/03_gold_processing.py`
5. `databricks/04_validation_queries.sql`
6. Use `databricks/dashboard_queries.sql` to create AI/BI dashboard datasets.

Detailed click-by-click instructions are in `docs/databricks_runbook.md`.

For Databricks Free Edition/serverless, the streaming writes use
`trigger(availableNow=True)`, so rerun/schedule notebooks for fresh micro-batch
updates.

## Local Tests

```bash
cd financial-streaming-demo
python3 -m unittest discover -s tests
```
