# app/ingestion/adapters/halyk_adapter.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from openpyxl import load_workbook

from app.ingestion.adapters.base_adapter import BankAdapter
from app.ingestion.metadata.statement_meta_extractor import StatementMetadataExtractor
from app.utils.text_utils import norm_text


@dataclass
class SimpleBlock:
    sheet_name: str
    header_row_idx: int
    data_start_row_idx: int
    data_end_row_idx: int


HALYK_HEADER_MARKERS = [
    "дата и время операции",
    "дата операции",
    "дата/время",
]


class HalykAdapter(BankAdapter):
    bank_name = "halyk"

    def __init__(self):
        self.meta = StatementMetadataExtractor()

    def list_files(self, data_root: str) -> List[str]:
        import os
        path = os.path.join(data_root, "halyk")
        if not os.path.exists(path):
            return []
        return [os.path.join(path, f) for f in os.listdir(path) if f.lower().endswith(".xlsx")]

    def load_grid(self, path: str, sheet_name: str) -> List[List[Any]]:
        wb = load_workbook(path, data_only=True, read_only=True)
        ws = wb[sheet_name]
        grid: List[List[Any]] = []
        max_col = ws.max_column
        for row in ws.iter_rows(values_only=True):
            grid.append(list(row[:max_col]))
        return grid

    def _is_header_row(self, row: List[Any]) -> bool:
        joined = " ".join(norm_text(x) for x in row if x is not None)
        if not joined:
            return False
        return any(m in joined for m in HALYK_HEADER_MARKERS)

    def _scan_end(self, grid: List[List[Any]], start_idx: int) -> int:
        n = len(grid)
        empty_streak = 0
        last_data = start_idx - 1

        for r in range(start_idx, n):
            row = grid[r]

            # next table starts
            if self._is_header_row(row):
                break

            tokens = [norm_text(c) for c in row]
            if all(t == "" for t in tokens):
                empty_streak += 1
            else:
                empty_streak = 0
                last_data = r

            if empty_streak >= 3:
                break

        return max(last_data, start_idx - 1)

    def _detect_blocks(self, grid: List[List[Any]], sheet_name: str) -> List[SimpleBlock]:
        blocks: List[SimpleBlock] = []
        i = 0
        n = len(grid)
        while i < n:
            if self._is_header_row(grid[i]):
                header_i = i
                data_start = i + 1
                end = self._scan_end(grid, data_start)
                if end >= data_start:
                    blocks.append(SimpleBlock(sheet_name, header_i, data_start, end))
                i = end + 1
            else:
                i += 1
        return blocks

    def _build_df(self, grid: List[List[Any]], block: SimpleBlock) -> pd.DataFrame:
        header = [norm_text(x) for x in grid[block.header_row_idx]]
        data = grid[block.data_start_row_idx : block.data_end_row_idx + 1]
        df = pd.DataFrame(data, columns=header)
        df = df.dropna(axis=0, how="all")
        df = df.dropna(axis=1, how="all")
        return df

    def extract(self, file_path: str) -> List[Tuple[pd.DataFrame, Dict[str, Any]]]:
        wb = load_workbook(file_path, data_only=True, read_only=True)
        out: List[Tuple[pd.DataFrame, Dict[str, Any]]] = []

        for sheet_name in wb.sheetnames:
            grid = self.load_grid(file_path, sheet_name)
            blocks = self._detect_blocks(grid, sheet_name)

            for bidx, b in enumerate(blocks, start=1):
                df = self._build_df(grid, b)
                if df.empty or len(df.columns) < 3:
                    continue

                # make DetectedBlock-like object for StatementMetadataExtractor
                # It only needs .header_row_idx
                class _B:
                    header_row_idx = b.header_row_idx

                stmt_meta = self.meta.extract_for_block(
                    grid=grid,
                    block=_B(),  # type: ignore[arg-type]
                    source_bank=self.bank_name,
                    max_lookback_rows=80,
                )

                stmt_meta["source_sheet"] = sheet_name
                stmt_meta["source_block_id"] = bidx
                stmt_meta["source_row_base"] = b.data_start_row_idx
                stmt_meta["meta_json"] = (stmt_meta.get("meta_json") or {})
                stmt_meta["meta_json"]["source"] = "halyk_adapter"

                out.append((df, stmt_meta))

        return out