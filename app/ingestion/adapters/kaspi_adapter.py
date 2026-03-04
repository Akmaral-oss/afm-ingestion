from __future__ import annotations

import os
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
from openpyxl import load_workbook

from app.ingestion.adapters.base_adapter import BankAdapter
from app.ingestion.extractor.block_detector import DetectedBlock
from app.ingestion.metadata.statement_meta_extractor import StatementMetadataExtractor
from app.utils.text_utils import norm_text


GROUP_HEADERS = {"плательщик": "payer", "получатель": "receiver"}

SUB_HEADERS = {
    "наименование/фио": "name",
    "иин/бин": "iin_bin",
    "резидентство": "residency",
    "банк": "bank",
    "бик": "bic",
    "номер счета": "account",
    "счет": "account",
}

SIMPLE_HEADER_MARKERS = [
    "дата и время операции",
    "дата операции",
    "дата/время",
]

# ✅ Footer markers (Kaspi ends table with these)
FOOTER_PRIMARY = ["итого", "итого оборотов", "исходящий остаток", "входящий остаток"]
FOOTER_SECONDARY = ["дебет", "кредит", "оборот", "итого"]


class KaspiAdapter(BankAdapter):
    bank_name = "kaspi"

    def __init__(self) -> None:
        self.meta_extractor = StatementMetadataExtractor()

    def list_files(self, data_root: str) -> List[str]:
        path = os.path.join(data_root, "kaspi")
        if not os.path.exists(path):
            return []
        return [
            os.path.join(path, f)
            for f in os.listdir(path)
            if f.lower().endswith(".xlsx")
        ]

    def _load_workbook(self, path: str):
        return load_workbook(path, data_only=True, read_only=True)

    def load_grid(self, path: str, sheet_name: str) -> List[List[Any]]:
        wb = self._load_workbook(path)
        ws = wb[sheet_name]
        grid: List[List[Any]] = []
        max_col = ws.max_column
        for row in ws.iter_rows(values_only=True):
            grid.append(list(row[:max_col]))
        return grid

    def _is_all_empty(self, row: List[Any]) -> bool:
        for x in row:
            if x is None:
                continue
            if str(x).strip() != "":
                return False
        return True

    def _scan_end(self, grid: List[List[Any]], start: int) -> int:
        empty_streak = 0
        last_data = start - 1
        for r in range(start, len(grid)):
            row = grid[r]
            if self._is_all_empty(row):
                empty_streak += 1
            else:
                empty_streak = 0
                last_data = r
            if empty_streak >= 3:
                break
        return max(last_data, start - 1)

    def _is_group_header_row(self, row: List[Any]) -> bool:
        tokens = [norm_text(x) for x in row]
        return ("плательщик" in tokens) and ("получатель" in tokens)

    def _is_subheader_row(self, row: List[Any]) -> bool:
        tokens = [norm_text(x) for x in row]
        hits = sum(1 for k in SUB_HEADERS.keys() if k in tokens)
        return hits >= 3

    def _is_simple_header_row(self, row: List[Any]) -> bool:
        joined = " ".join(norm_text(x) for x in row if x is not None)
        if not joined:
            return False
        return any(m in joined for m in SIMPLE_HEADER_MARKERS)

    def combine_headers(self, group_row: List[Any], sub_row: List[Any]) -> List[str]:
        headers: List[str] = []
        current_group: Optional[str] = None
        m = max(len(group_row), len(sub_row))

        for j in range(m):
            g = norm_text(group_row[j]) if j < len(group_row) else ""
            s = norm_text(sub_row[j]) if j < len(sub_row) else ""

            if g in GROUP_HEADERS:
                current_group = GROUP_HEADERS[g]
                headers.append(g or s or f"col_{j}")
                continue

            if current_group and s in SUB_HEADERS:
                headers.append(f"{current_group}/{SUB_HEADERS[s]}")
            else:
                headers.append(g or s or f"col_{j}")

            if current_group and g and (g not in GROUP_HEADERS) and not s:
                current_group = None

        return headers

    # ✅ NEW: detect where footer starts INSIDE the table block
    def _find_footer_start(self, grid: List[List[Any]], data_start: int, data_end: int) -> Optional[int]:
        """
        Kaspi often has totals inside the same table block:
          - "Итого оборотов ..."
          - "Исходящий остаток: ..."
        We must cut transactions before these rows.
        """
        for r in range(data_start, data_end + 1):
            joined = " ".join(norm_text(c) for c in grid[r] if c is not None)
            if not joined:
                continue

            # strong triggers
            if any(k in joined for k in FOOTER_PRIMARY):
                # avoid accidental "кредит" inside operation text: require at least one secondary token too
                if any(k in joined for k in FOOTER_SECONDARY):
                    return r
                # "исходящий остаток" alone is enough
                if "исходящий остаток" in joined or "входящий остаток" in joined:
                    return r

        return None

    def extract(self, file_path: str) -> List[Tuple[pd.DataFrame, Dict[str, Any]]]:
        """
        Returns: List[(df, stmt_meta)]
        stmt_meta includes statement fields extracted by StatementMetadataExtractor
        """
        wb = self._load_workbook(file_path)
        out: List[Tuple[pd.DataFrame, Dict[str, Any]]] = []

        for sheet_name in wb.sheetnames:
            grid = self.load_grid(file_path, sheet_name)

            block_id = 0
            i = 0
            while i < len(grid) - 1:

                # ---------------------------
                # Mode A: grouped header (2 rows)
                # ---------------------------
                if self._is_group_header_row(grid[i]) and self._is_subheader_row(grid[i + 1]):
                    headers = self.combine_headers(grid[i], grid[i + 1])

                    data_start = i + 2
                    data_end = self._scan_end(grid, data_start)
                    if data_end < data_start:
                        i += 1
                        continue

                    # ✅ CUT footer if it's inside the table
                    footer_start = self._find_footer_start(grid, data_start, data_end)
                    tx_end = (footer_start - 1) if footer_start is not None else data_end

                    if tx_end < data_start:
                        i = data_end + 1
                        continue

                    rows = grid[data_start : tx_end + 1]
                    df = pd.DataFrame(rows, columns=headers).dropna(how="all")
                    df = df.dropna(axis=1, how="all")

                    if not df.empty and len(df.columns) >= 3:
                        block_id += 1

                        # ✅ block ends at tx_end (footer will be in lookahead window)
                        block = DetectedBlock(
                            sheet_name=sheet_name,
                            header_row_idx=i,
                            data_start_row_idx=data_start,
                            data_end_row_idx=tx_end,
                            header_rows=2,
                            group_row_idx=i,
                            subheader_row_idx=i + 1,
                        )

                        stmt_meta = self.meta_extractor.extract_for_block(
                            grid=grid,
                            block=block,
                            source_bank="kaspi",
                            max_lookback_rows=80,
                            max_lookahead_rows=80,  # ✅ allow footer search
                        )

                        stmt_meta.update({
                            "source_sheet": sheet_name,
                            "source_block_id": block_id,
                            "source_row_base": data_start,
                            "meta_json": {
                                **(stmt_meta.get("meta_json") or {}),
                                "source": "kaspi_adapter",
                                "mode": "grouped_header",
                                "header_row_idx": i,
                                "data_start": data_start,
                                "tx_end": tx_end,
                                "data_end_raw": data_end,
                                "footer_start": footer_start,
                            },
                        })

                        out.append((df, stmt_meta))

                    i = data_end + 1
                    continue

                # ---------------------------
                # Mode B: simple header (1 row)
                # ---------------------------
                if self._is_simple_header_row(grid[i]):
                    header = [norm_text(x) for x in grid[i]]
                    data_start = i + 1
                    data_end = self._scan_end(grid, data_start)
                    if data_end < data_start:
                        i += 1
                        continue

                    footer_start = self._find_footer_start(grid, data_start, data_end)
                    tx_end = (footer_start - 1) if footer_start is not None else data_end

                    if tx_end < data_start:
                        i = data_end + 1
                        continue

                    rows = grid[data_start : tx_end + 1]
                    df = pd.DataFrame(rows, columns=header).dropna(how="all")
                    df = df.dropna(axis=1, how="all")

                    if not df.empty and len(df.columns) >= 3:
                        block_id += 1

                        block = DetectedBlock(
                            sheet_name=sheet_name,
                            header_row_idx=i,
                            data_start_row_idx=data_start,
                            data_end_row_idx=tx_end,
                            header_rows=1,
                            group_row_idx=None,
                            subheader_row_idx=None,
                        )

                        stmt_meta = self.meta_extractor.extract_for_block(
                            grid=grid,
                            block=block,
                            source_bank="kaspi",
                            max_lookback_rows=80,
                            max_lookahead_rows=80,
                        )

                        stmt_meta.update({
                            "source_sheet": sheet_name,
                            "source_block_id": block_id,
                            "source_row_base": data_start,
                            "meta_json": {
                                **(stmt_meta.get("meta_json") or {}),
                                "source": "kaspi_adapter",
                                "mode": "simple_header",
                                "header_row_idx": i,
                                "data_start": data_start,
                                "tx_end": tx_end,
                                "data_end_raw": data_end,
                                "footer_start": footer_start,
                            },
                        })

                        out.append((df, stmt_meta))

                    i = data_end + 1
                    continue

                i += 1

        return out