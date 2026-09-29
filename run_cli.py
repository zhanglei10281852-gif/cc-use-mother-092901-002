import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from event_fusion.contracts import EventObservation, ObservationKind, SourceRef


source = SourceRef("rsu", "obs-18", datetime(2026, 10, 21, 2, tzinfo=timezone.utc))
item = EventObservation("EV-9", ObservationKind.LANE_CLOSED, "lane-3", source, 0.91)
print(json.dumps({"event_key": item.event_key, "kind": item.kind.value, "confidence": item.confidence}, ensure_ascii=False))
