from __future__ import annotations

import json
import os
import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


DEFAULT_SYMBOLS = ["AAPL", "MSFT", "NVDA", "TSLA", "BTCUSD", "ETHUSD"]
DEFAULT_EXCHANGES = ["NASDAQ", "NYSE", "CRYPTO"]


@dataclass(frozen=True)
class AnomalyConfig:
    price_jump_probability: float = 0.03
    volume_spike_probability: float = 0.03
    wide_spread_probability: float = 0.02
    delayed_event_probability: float = 0.02
    duplicate_probability: float = 0.01
    max_delay_seconds: int = 120


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = ""
    topic: str = "financial_ticks"
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "PLAIN"
    username: str = ""
    password: str = ""


@dataclass(frozen=True)
class ProducerConfig:
    symbols: list[str] = field(default_factory=lambda: DEFAULT_SYMBOLS.copy())
    exchanges: list[str] = field(default_factory=lambda: DEFAULT_EXCHANGES.copy())
    random_seed: int | None = 42
    output_dir: str = "data/raw_ticks"
    file_events_per_chunk: int = 25


@dataclass(frozen=True)
class AppConfig:
    producer: ProducerConfig = field(default_factory=ProducerConfig)
    anomalies: AnomalyConfig = field(default_factory=AnomalyConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)


def load_config(path: str | None) -> AppConfig:
    if not path:
        raw: dict[str, Any] = {}
    else:
        config_path = Path(path)
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        raw = _load_mapping(config_path)

    kafka_raw = dict(raw.get("kafka", {}))
    kafka_raw["username"] = os.getenv("KAFKA_USERNAME", kafka_raw.get("username", ""))
    kafka_raw["password"] = os.getenv("KAFKA_PASSWORD", kafka_raw.get("password", ""))

    return AppConfig(
        producer=ProducerConfig(**raw.get("producer", {})),
        anomalies=AnomalyConfig(**raw.get("anomalies", {})),
        kafka=KafkaConfig(**kafka_raw),
    )


def _load_mapping(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(text)

    try:
        import yaml  # type: ignore
    except ImportError as exc:
        if path.suffix.lower() in {".yaml", ".yml"}:
            return _load_simple_yaml(text)
        raise RuntimeError("YAML config requires PyYAML. Install requirements.txt or use JSON.") from exc

    data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config must contain a mapping/object: {path}")
    return data


def _load_simple_yaml(text: str) -> dict[str, Any]:
    """Small fallback parser for this project's example YAML config."""
    root: dict[str, Any] = {}
    current_section: dict[str, Any] | None = None

    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue

        if not raw_line.startswith(" ") and line.endswith(":"):
            section_name = line[:-1].strip()
            current_section = {}
            root[section_name] = current_section
            continue

        if current_section is None or ":" not in line:
            raise ValueError("Unsupported YAML shape. Install PyYAML for full YAML support.")

        key, value = line.strip().split(":", 1)
        current_section[key.strip()] = _parse_scalar(value.strip())

    return root


def _parse_scalar(value: str) -> Any:
    if value in {"", "null", "None"}:
        return None
    if value in {"true", "True"}:
        return True
    if value in {"false", "False"}:
        return False
    if value.startswith("[") or value.startswith("{") or value.startswith('"') or value.startswith("'"):
        return ast.literal_eval(value)
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value
