USE CATALOG student_streaming;
USE SCHEMA market_demo;

-- Latest price and source freshness.
SELECT
  symbol,
  latest_event_time,
  last_price,
  total_events,
  total_volume,
  total_anomalies,
  max_event_delay_seconds,
  freshness_status
FROM gold_market_health
ORDER BY symbol;

-- Price, VWAP, and volume trend.
SELECT
  window_start,
  window_end,
  symbol,
  trade_count,
  total_volume,
  avg_price,
  min_price,
  max_price,
  vwap,
  avg_spread,
  max_event_delay_seconds
FROM gold_symbol_kpis
ORDER BY window_start DESC, symbol;

-- Anomaly table.
SELECT
  event_time,
  symbol,
  anomaly_type,
  observed_value,
  threshold,
  severity,
  anomaly_hint
FROM gold_anomalies
ORDER BY event_time DESC;

-- Anomaly count by type.
SELECT
  anomaly_type,
  severity,
  COUNT(*) AS anomaly_count
FROM gold_anomalies
GROUP BY anomaly_type, severity
ORDER BY anomaly_count DESC;

