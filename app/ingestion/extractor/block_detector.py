from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional

import pandas as pd

from app.utils.text_utils import norm_text

HEADER_KEYWORDS = [
    "дата",
    "время",
    "операц",
    "валют",
    "сумм",
    "тенге",
    "назначен",
    "плательщик",
    "получатель",
    "иин",
    "бин",
    "счет",
    "номер счета",
    "банк",
    "резидент",
    "код назначения",
    "виды операции",
    "категория",
    "sdp",
    "сдп",
    "transaction",
    "date",
    "amount",
    "currency",
    "purpose",
    "payer",
    "receiver",
]

KASPI_GROUP_HEADERS = {"плательщик": "payer", "получатель": "receiver"}
KASPI_SUBHEADERS = {
    "наименование/фио": "name",
    "иин/бин": "iin_bin",
    "резидентство": "residency",
    "банк": "bank",
    "бик": "bic",
    "номер счета": "account",
    "счет": "account",
}


@dataclass
class DetectedBlock:
    sheet_name: str
    header_row_idx: int
    data_start_row_idx: int
    data_end_row_idx: int
    header_rows: int
    group_row_idx: Optional[int] = None
    subheader_row_idx: Optional[int] = None


def _is_header_row(row: List[Any]) -> bool:
    tokens = [norm_text(c) for c in row]
    joined = " ".join(tokens)
    keys = [norm_text(k) for k in HEADER_KEYWORDS]
    hits = sum(1 for k in keys if k and k in joined)
    nonempty = sum(1 for t in tokens if t)
    return (hits >= 3) and (nonempty >= 6)


def _is_kaspi_group_row(row: List[Any]) -> bool:
    tokens = [norm_text(c) for c in row]
    return ("плательщик" in tokens) and ("получатель" in tokens)


def _is_kaspi_subheader_row(row: List[Any]) -> bool:
    tokens = [norm_text(c) for c in row]
    hits = sum(1 for k in KASPI_SUBHEADERS.keys() if k in tokens)
    return hits >= 3


def _scan_until_end(grid: List[List[Any]], start_idx: int) -> int:
    n = len(grid)
    empty_streak = 0
    last_data = start_idx - 1

    for r in range(start_idx, n):
        row = grid[r]

        # Stop if next table starts
        if _is_header_row(row) or _is_kaspi_group_row(row):
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


def detect_blocks(grid: List[List[Any]], sheet_name: str) -> List[DetectedBlock]:
    blocks: List[DetectedBlock] = []
    n = len(grid)
    i = 0

    while i < n:
        row = grid[i]

        # Kaspi grouped header (2 header rows)
        if (
            _is_kaspi_group_row(row)
            and (i + 1 < n)
            and _is_kaspi_subheader_row(grid[i + 1])
        ):
            data_start = i + 2
            end = _scan_until_end(grid, data_start)
            blocks.append(
                DetectedBlock(
                    sheet_name=sheet_name,
                    header_row_idx=i,
                    data_start_row_idx=data_start,
                    data_end_row_idx=end,
                    header_rows=2,
                    group_row_idx=i,
                    subheader_row_idx=i + 1,
                )
            )
            i = end + 1
            continue

        # Normal header (1 header row)
        if _is_header_row(row):
            data_start = i + 1
            end = _scan_until_end(grid, data_start)
            blocks.append(
                DetectedBlock(
                    sheet_name=sheet_name,
                    header_row_idx=i,
                    data_start_row_idx=data_start,
                    data_end_row_idx=end,
                    header_rows=1,
                )
            )
            i = end + 1
            continue

        i += 1

    return blocks


def build_df_from_block(grid: List[List[Any]], block: DetectedBlock) -> pd.DataFrame:
    # 1-row header
    if block.header_rows == 1:
        header = [norm_text(x) for x in grid[block.header_row_idx]]
        data = grid[block.data_start_row_idx : block.data_end_row_idx + 1]
        return pd.DataFrame(data, columns=header)

    # 2-row grouped header (Kaspi payer/receiver)
    top = [norm_text(x) for x in grid[block.group_row_idx]]  # type: ignore[arg-type]
    sub = [norm_text(x) for x in grid[block.subheader_row_idx]]  # type: ignore[arg-type]

    combined: List[str] = []
    current_group: Optional[str] = None

    for j in range(max(len(top), len(sub))):
        t = top[j] if j < len(top) else ""
        s = sub[j] if j < len(sub) else ""

        # ✅ FIX: if group header cell occurs, build "payer/<sub>" instead of raw "плательщик"
        if t in KASPI_GROUP_HEADERS:
            current_group = KASPI_GROUP_HEADERS[t]
            if s in KASPI_SUBHEADERS:
                combined.append(f"{current_group}/{KASPI_SUBHEADERS[s]}")
            else:
                combined.append(f"{current_group}/name")  # safe default
            continue

        if current_group and s in KASPI_SUBHEADERS:
            combined.append(f"{current_group}/{KASPI_SUBHEADERS[s]}")
        else:
            combined.append(t or s or f"col_{j}")

        # reset group if ended
        if current_group and (s == "" and t != "") and (t not in KASPI_GROUP_HEADERS):
            current_group = None

    data = grid[block.data_start_row_idx : block.data_end_row_idx + 1]
    return pd.DataFrame(data, columns=combined)