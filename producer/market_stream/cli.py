from __future__ import annotations

import argparse
import time

from .config import load_config
from .generator import TickGenerator
from .sinks import JsonFileSink, KafkaSink


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mock financial tick producer")
    parser.add_argument("--mode", choices=["files", "kafka"], default="files")
    parser.add_argument("--config", default="configs/local.example.yaml")
    parser.add_argument("--events-per-second", type=float, default=5.0)
    parser.add_argument("--duration-seconds", type=int, default=60)
    parser.add_argument("--inject-anomalies", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    if args.events_per_second <= 0:
        raise ValueError("--events-per-second must be greater than zero")

    sink = (
        KafkaSink(config.kafka)
        if args.mode == "kafka"
        else JsonFileSink(
            config.producer.output_dir,
            events_per_chunk=config.producer.file_events_per_chunk,
        )
    )
    generator = TickGenerator(config.producer, config.anomalies)
    total_events = int(args.events_per_second * args.duration_seconds)
    sleep_seconds = 1.0 / args.events_per_second

    print(
        f"Producing {total_events} events in {args.mode} mode "
        f"({args.events_per_second:g} events/sec)"
    )
    try:
        for index, event in enumerate(
            generator.stream(total_events, inject_anomalies=args.inject_anomalies), start=1
        ):
            sink.write(event)
            if index % max(int(args.events_per_second), 1) == 0:
                print(
                    f"sent={index} symbol={event['symbol']} "
                    f"price={event['price']} hint={event['anomaly_hint']}"
                )
            time.sleep(sleep_seconds)
    finally:
        sink.close()


if __name__ == "__main__":
    main()

