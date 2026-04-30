USE CATALOG student_streaming;
USE SCHEMA market_demo;

-- Check whether Bronze raw JSON can be parsed into the expected fields.
WITH parsed AS (
  SELECT
    from_json(
      raw_json,
      'event_id STRING, symbol STRING, event_time STRING, price DOUBLE, volume BIGINT, bid DOUBLE, ask DOUBLE, exchange STRING, sequence_number BIGINT, producer_time STRING, anomaly_hint STRING'
    ) AS tick,
    ingestion_time
  FROM bronze_ticks_raw
)
SELECT
  COUNT(*) AS bronze_rows_checked,
  COUNT(tick.event_id) AS parsed_event_ids,
  COUNT(tick.symbol) AS parsed_symbols,
  COUNT(tick.event_time) AS parsed_event_time_strings,
  COUNT(to_timestamp(tick.event_time, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")) AS parsed_event_timestamps,
  COUNT(tick.price) AS parsed_prices,
  COUNT(tick.volume) AS parsed_volumes
FROM parsed;

-- Show a few parsed examples.
WITH parsed AS (
  SELECT
    from_json(
      raw_json,
      'event_id STRING, symbol STRING, event_time STRING, price DOUBLE, volume BIGINT, bid DOUBLE, ask DOUBLE, exchange STRING, sequence_number BIGINT, producer_time STRING, anomaly_hint STRING'
    ) AS tick,
    ingestion_time
  FROM bronze_ticks_raw
)
SELECT
  tick.event_id,
  tick.symbol,
  tick.event_time,
  to_timestamp(tick.event_time, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'") AS parsed_event_time,
  tick.price,
  tick.volume,
  tick.anomaly_hint
FROM parsed
LIMIT 20;
