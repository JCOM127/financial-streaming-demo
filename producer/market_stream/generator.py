from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Iterator

from .config import AnomalyConfig, ProducerConfig


BASE_PRICES = {
    "AAPL": 185.0,
    "MSFT": 420.0,
    "NVDA": 875.0,
    "TSLA": 175.0,
    "BTCUSD": 66500.0,
    "ETHUSD": 3450.0,
}


@dataclass
class TickGenerator:
    producer_config: ProducerConfig
    anomaly_config: AnomalyConfig

    def __post_init__(self) -> None:
        self._rng = random.Random(self.producer_config.random_seed)
        self._prices = {
            symbol: BASE_PRICES.get(symbol, self._rng.uniform(50, 500))
            for symbol in self.producer_config.symbols
        }
        self._sequence_by_symbol = {symbol: 0 for symbol in self.producer_config.symbols}
        self._last_event: dict[str, Any] | None = None

    def stream(self, count: int | None = None, inject_anomalies: bool = False) -> Iterator[dict[str, Any]]:
        emitted = 0
        while count is None or emitted < count:
            tick = self.next_tick(inject_anomalies=inject_anomalies)
            emitted += 1
            yield tick

    def next_tick(self, inject_anomalies: bool = False) -> dict[str, Any]:
        if (
            inject_anomalies
            and self._last_event is not None
            and self._chance(self.anomaly_config.duplicate_probability)
        ):
            duplicate = dict(self._last_event)
            duplicate["producer_time"] = _now_iso()
            duplicate["anomaly_hint"] = "duplicate_event"
            return duplicate

        symbol = self._rng.choice(self.producer_config.symbols)
        self._sequence_by_symbol[symbol] += 1

        price = self._next_price(symbol)
        volume = self._rng.randint(10, 2_000)
        spread = max(price * self._rng.uniform(0.0001, 0.0015), 0.01)
        event_time = datetime.now(UTC)
        anomaly_hint = "normal"

        if inject_anomalies and self._chance(self.anomaly_config.price_jump_probability):
            direction = self._rng.choice([-1, 1])
            price *= 1 + direction * self._rng.uniform(0.05, 0.18)
            anomaly_hint = "price_jump"

        if inject_anomalies and self._chance(self.anomaly_config.volume_spike_probability):
            volume *= self._rng.randint(8, 25)
            anomaly_hint = "volume_spike"

        if inject_anomalies and self._chance(self.anomaly_config.wide_spread_probability):
            spread = price * self._rng.uniform(0.02, 0.08)
            anomaly_hint = "wide_spread"

        if inject_anomalies and self._chance(self.anomaly_config.delayed_event_probability):
            delay = self._rng.randint(15, self.anomaly_config.max_delay_seconds)
            event_time -= timedelta(seconds=delay)
            anomaly_hint = "delayed_event"

        bid = price - spread / 2
        ask = price + spread / 2
        exchange = self._exchange_for(symbol)

        tick = {
            "event_id": str(uuid.uuid4()),
            "symbol": symbol,
            "event_time": event_time.isoformat().replace("+00:00", "Z"),
            "price": round(price, 4),
            "volume": int(volume),
            "bid": round(bid, 4),
            "ask": round(ask, 4),
            "exchange": exchange,
            "sequence_number": self._sequence_by_symbol[symbol],
            "producer_time": _now_iso(),
            "anomaly_hint": anomaly_hint,
        }
        self._last_event = tick
        return tick

    def _next_price(self, symbol: str) -> float:
        current = self._prices[symbol]
        drift = self._rng.gauss(0, current * 0.001)
        updated = max(current + drift, 0.01)
        self._prices[symbol] = updated
        return updated

    def _exchange_for(self, symbol: str) -> str:
        if symbol.endswith("USD"):
            return "CRYPTO"
        return self._rng.choice([exchange for exchange in self.producer_config.exchanges if exchange != "CRYPTO"])

    def _chance(self, probability: float) -> bool:
        return self._rng.random() < probability


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")

