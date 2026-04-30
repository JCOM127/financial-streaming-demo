# Databricks notebook source
# Generate mock financial tick JSONL files directly into a Unity Catalog volume.
#
# Run this after 00_setup.sql and before 01_bronze_ingestion.py.

import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

dbutils.widgets.text("output_path", "/Volumes/student_streaming/market_demo/raw_ticks")
dbutils.widgets.text("events_per_second", "5")
dbutils.widgets.text("duration_seconds", "60")
dbutils.widgets.text("file_events_per_chunk", "25")
dbutils.widgets.dropdown("inject_anomalies", "true", ["true", "false"])
dbutils.widgets.text("random_seed", "42")

output_path = dbutils.widgets.get("output_path").rstrip("/")
events_per_second = float(dbutils.widgets.get("events_per_second"))
duration_seconds = int(dbutils.widgets.get("duration_seconds"))
file_events_per_chunk = int(dbutils.widgets.get("file_events_per_chunk"))
inject_anomalies = dbutils.widgets.get("inject_anomalies").lower() == "true"
random_seed = int(dbutils.widgets.get("random_seed"))

symbols = ["AAPL", "MSFT", "NVDA", "TSLA", "BTCUSD", "ETHUSD"]
base_prices = {
    "AAPL": 185.0,
    "MSFT": 420.0,
    "NVDA": 875.0,
    "TSLA": 175.0,
    "BTCUSD": 66500.0,
    "ETHUSD": 3450.0,
}

rng = random.Random(random_seed)
prices = dict(base_prices)
sequence_by_symbol = {symbol: 0 for symbol in symbols}
last_event = None
total_events = int(events_per_second * duration_seconds)
sleep_seconds = 1.0 / events_per_second if events_per_second > 0 else 0

dbutils.fs.mkdirs(output_path)


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def chance(probability):
    return rng.random() < probability


def exchange_for(symbol):
    if symbol.endswith("USD"):
        return "CRYPTO"
    return rng.choice(["NASDAQ", "NYSE"])


def next_tick():
    global last_event

    if inject_anomalies and last_event is not None and chance(0.01):
        duplicate = dict(last_event)
        duplicate["producer_time"] = now_iso()
        duplicate["anomaly_hint"] = "duplicate_event"
        return duplicate

    symbol = rng.choice(symbols)
    sequence_by_symbol[symbol] += 1

    current = prices[symbol]
    price = max(current + rng.gauss(0, current * 0.001), 0.01)
    prices[symbol] = price

    volume = rng.randint(10, 2_000)
    spread = max(price * rng.uniform(0.0001, 0.0015), 0.01)
    event_time = datetime.now(timezone.utc)
    anomaly_hint = "normal"

    if inject_anomalies and chance(0.03):
        direction = rng.choice([-1, 1])
        price *= 1 + direction * rng.uniform(0.05, 0.18)
        anomaly_hint = "price_jump"

    if inject_anomalies and chance(0.03):
        volume *= rng.randint(8, 25)
        anomaly_hint = "volume_spike"

    if inject_anomalies and chance(0.02):
        spread = price * rng.uniform(0.02, 0.08)
        anomaly_hint = "wide_spread"

    if inject_anomalies and chance(0.02):
        event_time -= timedelta(seconds=rng.randint(15, 120))
        anomaly_hint = "delayed_event"

    bid = price - spread / 2
    ask = price + spread / 2

    tick = {
        "event_id": str(uuid.uuid4()),
        "symbol": symbol,
        "event_time": event_time.isoformat().replace("+00:00", "Z"),
        "price": round(price, 4),
        "volume": int(volume),
        "bid": round(bid, 4),
        "ask": round(ask, 4),
        "exchange": exchange_for(symbol),
        "sequence_number": sequence_by_symbol[symbol],
        "producer_time": now_iso(),
        "anomaly_hint": anomaly_hint,
    }
    last_event = tick
    return tick


buffer = []
files_written = 0
events_written = 0

for event_number in range(1, total_events + 1):
    buffer.append(next_tick())

    if len(buffer) >= file_events_per_chunk or event_number == total_events:
        millis = int(time.time() * 1000)
        file_path = f"{output_path}/ticks_{millis}_{files_written:06d}.jsonl"
        payload = "\n".join(json.dumps(event, separators=(",", ":")) for event in buffer) + "\n"
        dbutils.fs.put(file_path, payload, overwrite=True)
        events_written += len(buffer)
        files_written += 1
        buffer = []

    if event_number % max(int(events_per_second), 1) == 0:
        print(f"generated={event_number}/{total_events}")

    if sleep_seconds > 0:
        time.sleep(sleep_seconds)

print(f"Wrote {events_written} events across {files_written} files into {output_path}")

