from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


@dataclass
class IngestionContext:
    file_id: str
    statement_id: str
    format_id: str
    source_bank: str
    source_sheet: str
    source_block_id: int
    source_row_base: int
    account_iban: Optional[str] = None
    store_raw_row_json: bool = False
