USE CATALOG student_streaming;
USE SCHEMA market_demo;

-- 1. Confirm all expected tables exist.
SHOW TABLES IN student_streaming.market_demo;

-- 2. Bronze row count and newest ingestion.
SELECT
  COUNT(*) AS bronze_rows,
  MAX(ingestion_time) AS latest_ingestion_time
FROM bronze_ticks_raw;

-- 3. Silver row count, deduplication, and delay range.
SELECT
  COUNT(*) AS silver_rows,
  COUNT(DISTINCT event_id) AS distinct_event_ids,
  MIN(event_time) AS earliest_event_time,
  MAX(event_time) AS latest_event_time,
  MAX(event_delay_seconds) AS max_event_delay_seconds
FROM silver_ticks_clean;

-- 4. Rows per symbol.
SELECT
  symbol,
  COUNT(*) AS events,
  MIN(price) AS min_price,
  MAX(price) AS max_price,
  SUM(volume) AS total_volume
FROM silver_ticks_clean
GROUP BY symbol
ORDER BY events DESC;

-- 5. Dashboard tables should be populated.
SELECT COUNT(*) AS kpi_rows FROM gold_symbol_kpis;
SELECT COUNT(*) AS anomaly_rows FROM gold_anomalies;
SELECT COUNT(*) AS market_health_rows FROM gold_market_health;

-- 6. Recent anomalies.
SELECT
  event_time,
  symbol,
  anomaly_type,
  observed_value,
  threshold,
  severity,
  anomaly_hint
FROM gold_anomalies
ORDER BY event_time DESC
LIMIT 25;

