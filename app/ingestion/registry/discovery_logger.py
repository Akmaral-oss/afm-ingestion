from __future__ import annotations
from typing import List, Dict, Any


class DiscoveryLogger:
    def __init__(self, writer):
        self.writer = writer

    def log(self, records: List[Dict[str, Any]]) -> None:
        self.writer.insert_discovery(records)
