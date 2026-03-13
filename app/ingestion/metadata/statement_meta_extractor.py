from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from app.ingestion.extractor.block_detector import DetectedBlock
from app.ingestion.metadata.meta_patterns import META_KEY_PATTERNS
from app.utils.text_utils import norm_text, looks_like_iban, looks_like_iin_bin
from app.utils.date_utils import parse_date, parse_period
from app.utils.number_utils import parse_decimal


_KZ_IBAN_RE = re.compile(r"\bKZ[A-Z0-9]{8,32}\b", re.IGNORECASE)
_IIN_RE = re.compile(r"\b\d{12}\b")

FOOTER_PRIMARY = [
    "итого",
    "итого оборотов",
    "исходящий остаток",
    "входящий остаток",
]

HEADER_STOP_MARKERS = [
    "дата и время операции",
    "дата операции",
    "дата/время",
]


def _row_text(row: List[Any], max_cols: int = 32) -> str:
    out: List[str] = []
    for c in row[:max_cols]:
        if c is None:
            continue
        s = str(c).strip()
        if s:
            out.append(s)
    return " | ".join(out)


def _norm_tokens(row: List[Any], max_cols: int = 32) -> List[str]:
    toks = [norm_text(c) for c in row[:max_cols] if c is not None]
    return [t for t in toks if t]


def _norm_join(row: List[Any], max_cols: int = 32) -> str:
    return " ".join(_norm_tokens(row, max_cols=max_cols))


def _looks_like_header_row(row: List[Any]) -> bool:
    joined = _norm_join(row)
    if not joined:
        return False
    return any(marker in joined for marker in HEADER_STOP_MARKERS)


def _first_number_to_right(row: List[Any], needle: str, max_look: int = 12) -> Optional[float]:
    nrow = [norm_text(c) for c in row]

    for i, t in enumerate(nrow):
        if not t:
            continue

        if needle in t:
            for j in range(i + 1, min(len(row), i + 1 + max_look)):
                v = parse_decimal(row[j])
                if v is None:
                    continue
                return float(v)

    return None


def _all_numbers(row: List[Any], max_cols: int = 40) -> List[float]:
    vals: List[float] = []

    for c in row[:max_cols]:
        if c is None:
            continue

        v = parse_decimal(c)
        if v is None:
            continue

        vals.append(float(v))

    return vals


def _money_score(x: float) -> float:
    ax = abs(x)

    if ax >= 1_000_000:
        return ax + 10_000_000
    if ax >= 10_000:
        return ax + 1_000_000
    if ax >= 1_000:
        return ax + 100_000

    return ax


class StatementMetadataExtractor:
    def _detect_iban(self, text: str) -> Optional[str]:
        s = re.sub(r"\s+", "", text.upper())
        m = _KZ_IBAN_RE.search(s)
        if m:
            return m.group(0)
        return None

    def _detect_iin(self, text: str) -> Optional[str]:
        m = _IIN_RE.search(text)
        if m:
            return m.group(0)
        return None

    def extract_for_block(
        self,
        grid: List[List[Any]],
        block: DetectedBlock,
        source_bank: str,
        max_lookback_rows: int = 15,
        max_lookahead_rows: int = 40,
        tail_rows_in_block: int = 40,
    ) -> Dict[str, Any]:

        header_i = block.header_row_idx

        start = max(0, header_i - max_lookback_rows)
        window_above = grid[start:header_i]

        footer_start = min(len(grid), block.data_end_row_idx + 1)
        footer_end = min(len(grid), footer_start + max_lookahead_rows)
        window_below_raw = grid[footer_start:footer_end]

        tail_start = max(block.data_start_row_idx, block.data_end_row_idx - tail_rows_in_block)
        window_tail = grid[tail_start:block.data_end_row_idx + 1]

        raw_pairs: Dict[str, Any] = {}
        raw_lines: List[str] = []
        best: Dict[str, Tuple[Any, float]] = {}

        def push_pair(k: str, v: Any):
            kk = norm_text(k)
            if not kk:
                return
            if v is None:
                return
            if str(v).strip() == "":
                return
            raw_pairs[kk] = v

        def set_best_money(k: str, v: Any):
            kk = norm_text(k)
            if not kk or v is None:
                return

            pv = parse_decimal(v)
            if pv is None:
                return

            score = _money_score(float(pv))
            if kk not in best or score > best[kk][1]:
                best[kk] = (pv, score)

        # =================================================
        # KASPI-SPECIFIC METADATA
        # =================================================
        if source_bank == "kaspi":
            for i, row in enumerate(window_above):
                cells = row[:10]
                raw_lines.append(_row_text(cells))

                c0 = str(cells[0]).strip() if len(cells) > 0 and cells[0] is not None else ""
                c2 = str(cells[2]).strip() if len(cells) > 2 and cells[2] is not None else ""

                label = norm_text(c0)

                next_row = window_above[i + 1] if i + 1 < len(window_above) else []
                next_val = str(next_row[0]).strip() if next_row and next_row[0] is not None else ""

                value = c2 if c2 else next_val

                if label == "клиент":
                    push_pair("клиент", value)

                elif "иин" in label or "бин" in label:
                    push_pair("иин/бин", value)

                elif label == "период" or label.startswith("период"):
                    push_pair("period", value)

                elif "счет" in label and "валюта" not in label and "тип" not in label:
                    push_pair("iban", value)

                elif "валюта счета" in label:
                    push_pair("currency", value)

                elif "тип счета" in label:
                    push_pair("account_type", value)

                elif "дата формирования" in label or "дата выписки" in label or "формирования" in label:
                    push_pair("statement_date", value)

                elif "входящий остаток" in label:
                    push_pair("opening_balance", value)

                elif "исходящий остаток" in label:
                    push_pair("closing_balance", value)

                txt = _row_text(cells)

                iban = self._detect_iban(txt)
                if iban and "iban" not in raw_pairs:
                    push_pair("iban", iban)

                detected_iin = self._detect_iin(txt)
                if detected_iin and "иин/бин" not in raw_pairs:
                    push_pair("иин/бин", detected_iin)

        # =================================================
        # GENERIC / HALYK-STYLE METADATA
        # =================================================
        else:
            for row in window_above:
                cells = row[:14]
                raw_lines.append(_row_text(cells))

                # key:value
                for c in cells:
                    if c is None:
                        continue

                    s = str(c).strip()
                    if ":" in s:
                        k, v = s.split(":", 1)
                        push_pair(k.strip(), v.strip())

                # label → value right
                for j in range(len(cells)):
                    c0 = cells[j]
                    if c0 is None:
                        continue

                    s0 = str(c0).strip()
                    if not s0.endswith(":"):
                        continue

                    for k in range(j + 1, min(len(cells), j + 6)):
                        cv = cells[k]
                        if cv is None:
                            continue

                        sv = str(cv).strip()
                        if sv:
                            push_pair(s0.strip(" :"), sv)
                            break

                for c in cells:
                    if c is None:
                        continue

                    s = re.sub(r"\s+", "", str(c).upper())
                    m = _KZ_IBAN_RE.search(s)
                    if m:
                        push_pair("iban", m.group(0))

        # =================================================
        # FOOTER PARSER
        # =================================================
        def parse_footer_rows(rows: List[List[Any]]):
            for row in rows:
                joined = _norm_join(row)
                if not joined:
                    continue

                raw_lines.append(_row_text(row))

                if not any(k in joined for k in FOOTER_PRIMARY):
                    continue

                if source_bank == "kaspi":
                    nums = _all_numbers(row)
                    money_candidates = [n for n in nums if abs(n) >= 1000]

                    if "исходящий остаток" in joined and money_candidates:
                        set_best_money("closing_balance", max(money_candidates, key=abs))

                    if "входящий остаток" in joined and money_candidates:
                        set_best_money("opening_balance", max(money_candidates, key=abs))

                    if "итого" in joined and len(money_candidates) >= 2:
                        sorted_money = sorted(money_candidates, key=lambda x: abs(x), reverse=True)
                        set_best_money("total_debit", sorted_money[0])
                        set_best_money("total_credit", sorted_money[1])
                    continue

                if "исходящий остаток" in joined:
                    v = _first_number_to_right(row, "исходящий остаток")
                    if v is None:
                        nums = _all_numbers(row)
                        v = max(nums, key=abs) if nums else None
                    set_best_money("closing_balance", v)

                if "входящий остаток" in joined:
                    v = _first_number_to_right(row, "входящий остаток")
                    if v is None:
                        nums = _all_numbers(row)
                        v = max(nums, key=abs) if nums else None
                    set_best_money("opening_balance", v)

                if "итого" in joined:
                    d = _first_number_to_right(row, "дебет")
                    c = _first_number_to_right(row, "кредит")

                    nums = _all_numbers(row)
                    if nums and (d is None or c is None):
                        nums_sorted = sorted(nums, key=lambda x: abs(x), reverse=True)
                        if d is None:
                            d = nums_sorted[0]
                        if c is None and len(nums_sorted) > 1:
                            c = nums_sorted[1]

                    set_best_money("total_debit", d)
                    set_best_money("total_credit", c)

        if source_bank == "kaspi":
            parse_footer_rows(window_tail)

        elif source_bank == "halyk":
            safe_rows: List[List[Any]] = []
            for row in window_below_raw:
                if _looks_like_header_row(row):
                    break
                safe_rows.append(row)
            parse_footer_rows(safe_rows)

        else:
            parse_footer_rows(window_tail)

        for k, (v, _) in best.items():
            raw_pairs[norm_text(k)] = v

        def find_value(keys: List[str]) -> Optional[Any]:
            norm_keys = [norm_text(k) for k in keys]

            for rk, rv in raw_pairs.items():
                if rk in norm_keys:
                    return rv

            for rk, rv in raw_pairs.items():
                for kk in norm_keys:
                    if rk.startswith(kk):
                        return rv

            for rk, rv in raw_pairs.items():
                for kk in norm_keys:
                    if kk in rk:
                        return rv

            return None

        stmt: Dict[str, Any] = {
            "client_name": None,
            "client_iin_bin": None,
            "contract_no": None,
            "account_iban": None,
            "account_type": None,
            "currency": None,
            "statement_date": None,
            "period_from": None,
            "period_to": None,
            "opening_balance": None,
            "closing_balance": None,
            "total_debit": None,
            "total_credit": None,
            "meta_json": {
                "raw_pairs": raw_pairs,
                "raw_lines": raw_lines[:300],
                "source_bank_hint": source_bank,
            },
        }

        v_client = find_value(META_KEY_PATTERNS["client"])
        if v_client:
            stmt["client_name"] = str(v_client)

        stmt["client_iin_bin"] = looks_like_iin_bin(find_value(META_KEY_PATTERNS["iin_bin"]))
        stmt["contract_no"] = find_value(["contract #"] + META_KEY_PATTERNS["contract"])

        v_acc = find_value(META_KEY_PATTERNS["account"]) or find_value(["iban"])
        stmt["account_iban"] = looks_like_iban(v_acc)

        v_cur = find_value(META_KEY_PATTERNS["currency"])
        if v_cur:
            stmt["currency"] = str(v_cur).upper()

        stmt["account_type"] = find_value(META_KEY_PATTERNS["account_type"])
        stmt["statement_date"] = parse_date(find_value(META_KEY_PATTERNS["statement_date"]))

        per = find_value(META_KEY_PATTERNS["period"])
        if per:
            stmt["period_from"], stmt["period_to"] = parse_period(per)

        stmt["opening_balance"] = parse_decimal(
            find_value(META_KEY_PATTERNS["opening_balance"]) or raw_pairs.get("opening_balance")
        )
        stmt["closing_balance"] = parse_decimal(
            find_value(META_KEY_PATTERNS["closing_balance"]) or raw_pairs.get("closing_balance")
        )
        stmt["total_debit"] = parse_decimal(
            find_value(META_KEY_PATTERNS["total_debit"]) or raw_pairs.get("total_debit")
        )
        stmt["total_credit"] = parse_decimal(
            find_value(META_KEY_PATTERNS["total_credit"]) or raw_pairs.get("total_credit")
        )

        return stmt