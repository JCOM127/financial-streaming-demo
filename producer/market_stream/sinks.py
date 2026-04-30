from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Protocol

from .config import KafkaConfig


class Sink(Protocol):
    def write(self, event: dict) -> None:
        ...

    def close(self) -> None:
        ...


class JsonFileSink:
    def __init__(self, output_dir: str, events_per_chunk: int = 25) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.events_per_chunk = max(events_per_chunk, 1)
        self._file_index = 0
        self._events_in_file = 0
        self._handle = None
        self._open_next_file()

    def write(self, event: dict) -> None:
        if self._events_in_file >= self.events_per_chunk:
            self._open_next_file()
        assert self._handle is not None
        self._handle.write(json.dumps(event, separators=(",", ":")) + "\n")
        self._handle.flush()
        self._events_in_file += 1

    def close(self) -> None:
        if self._handle:
            self._handle.close()
            self._handle = None

    def _open_next_file(self) -> None:
        self.close()
        millis = int(time.time() * 1000)
        path = self.output_dir / f"ticks_{millis}_{self._file_index:06d}.jsonl"
        self._file_index += 1
        self._events_in_file = 0
        self._handle = path.open("a", encoding="utf-8")


class KafkaSink:
    def __init__(self, config: KafkaConfig) -> None:
        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise RuntimeError(
                "Kafka mode requires confluent-kafka. Install requirements.txt first."
            ) from exc

        if not config.bootstrap_servers or "<" in config.bootstrap_servers:
            raise ValueError("Kafka bootstrap_servers must be configured.")

        producer_config = {
            "bootstrap.servers": config.bootstrap_servers,
            "security.protocol": config.security_protocol,
            "sasl.mechanisms": config.sasl_mechanism,
            "sasl.username": config.username,
            "sasl.password": config.password,
        }
        self.topic = config.topic
        self._producer = Producer(producer_config)

    def write(self, event: dict) -> None:
        key = str(event["symbol"]).encode("utf-8")
        value = json.dumps(event, separators=(",", ":")).encode("utf-8")
        self._producer.produce(self.topic, key=key, value=value)
        self._producer.poll(0)

    def close(self) -> None:
        self._producer.flush(10)

