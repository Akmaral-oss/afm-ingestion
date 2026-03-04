from __future__ import annotations

from typing import Any, List
from openpyxl import load_workbook

from app.ingestion.extractor.block_detector import detect_blocks
from app.ingestion.extractor.block_detector import DetectedBlock
from app.ingestion.extractor.block_detector import build_df_from_block


class ExcelUniversalExtractor:
    def load_sheet_grid(self, xlsx_path: str, sheet_name: str) -> List[List[Any]]:
        wb = load_workbook(xlsx_path, data_only=True, read_only=True)
        ws = wb[sheet_name]
        grid: List[List[Any]] = []
        max_col = ws.max_column
        for row in ws.iter_rows(values_only=True):
            grid.append(list(row[:max_col]))
        return grid

    def detect_blocks(
        self, grid: List[List[Any]], sheet_name: str
    ) -> List[DetectedBlock]:
        return detect_blocks(grid, sheet_name)

    def extract_block_df(self, grid: List[List[Any]], block: DetectedBlock):
        return build_df_from_block(grid, block)
