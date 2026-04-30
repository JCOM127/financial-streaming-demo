import unittest

from producer.market_stream.config import AnomalyConfig, ProducerConfig
from producer.market_stream.generator import TickGenerator


class TickGeneratorTest(unittest.TestCase):
    def test_tick_contains_required_fields(self) -> None:
        generator = TickGenerator(ProducerConfig(random_seed=7), AnomalyConfig())
        tick = generator.next_tick()

        for field in [
            "event_id",
            "symbol",
            "event_time",
            "price",
            "volume",
            "bid",
            "ask",
            "exchange",
            "sequence_number",
            "producer_time",
        ]:
            self.assertIn(field, tick)

        self.assertGreater(tick["price"], 0)
        self.assertGreater(tick["volume"], 0)
        self.assertLess(tick["bid"], tick["ask"])

    def test_duplicate_anomaly_reuses_event_id(self) -> None:
        config = AnomalyConfig(
            price_jump_probability=0,
            volume_spike_probability=0,
            wide_spread_probability=0,
            delayed_event_probability=0,
            duplicate_probability=1,
        )
        generator = TickGenerator(ProducerConfig(random_seed=11), config)
        first = generator.next_tick(inject_anomalies=True)
        duplicate = generator.next_tick(inject_anomalies=True)

        self.assertEqual(first["event_id"], duplicate["event_id"])
        self.assertEqual("duplicate_event", duplicate["anomaly_hint"])


if __name__ == "__main__":
    unittest.main()

