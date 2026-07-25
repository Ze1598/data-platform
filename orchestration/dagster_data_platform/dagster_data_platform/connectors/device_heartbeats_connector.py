"""device_heartbeats' JsonFileConnector subclass -- part of the
iot_telemetry batch group (tier 1), a synthetic multi-feed/multi-tier
batch used to prove batch_feed_hierarchy tiered extraction
(Backlog.md/Roadmap.md "Hierarchy-tiered, dependency-driven
master_pipeline execution"), not a real external system. The synthetic
JSON dropped into landing is already flat (no nested structs), so
flatten() is a pure pass-through -- unlike police_crimes_connector.py,
there's no real nested shape to unpack here.
"""

from pathlib import Path
from typing import Optional

import polars as pl

from connectors import JsonFileConnector


class Connector(JsonFileConnector):
    def __init__(self, *, landing_dir: Path, last_watermark: Optional[str] = None):
        super().__init__(landing_dir=landing_dir)
        self._last_watermark = last_watermark

    def flatten(self, raw: pl.DataFrame) -> pl.DataFrame:
        return raw
