import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from event_fusion.contracts import EventObservation, ObservationKind, SourceRef


class EventContractTests(unittest.TestCase):
    def test_observation_retains_source_time(self):
        instant = datetime(2026, 10, 21, tzinfo=timezone.utc)
        source = SourceRef("vehicle", "m-1", instant)
        item = EventObservation("E-1", ObservationKind.OBSTACLE, "L-2", source, 0.7)
        self.assertEqual(item.source.observed_at, instant)

    def test_confidence_range_is_checked(self):
        source = SourceRef("rsu", "m-2", datetime.now(timezone.utc))
        with self.assertRaises(ValueError):
            EventObservation("E-2", ObservationKind.CLEARED, "L-2", source, 1.2)


if __name__ == "__main__":
    unittest.main()
