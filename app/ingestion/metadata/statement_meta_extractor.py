from __future__ import annotations

"""
StatementMetadataExtractor — fixed version
Fixes vs original:
  FIX 3: Kaspi total_credit was extracted as "2" (the debit count from
          "Итого оборотов в вал | 2 дебет | 0 кредит").
          New: _parse_kaspi_totals_row() uses explicit regex to find
          "N дебет" and "N кредит" amounts, falling back to largest numbers
          only when the pattern is absent.
  FIX 4: client_name not extracted — norm_text("Клиент:") → "клиент:"
          (colon preserved), but comparison was == "клиент" (no colon).
          Fixed: strip trailing colon from label before all comparisons.
  FIX 5: account_type not extracted — same colon stripping fix.
"""

import re
from typing import Any, Dict, List, Optional, Tuple

from app.ingestion.extractor.block_detector import DetectedBlock
from app.ingestion.metadata.meta_patterns import META_KEY_PATTERNS
from app.utils.text_utils import norm_text, looks_like_iban, looks_like_iin_bin
from app.utils.date_utils import parse_date, parse_period
from app.utils.number_utils import parse_decimal


_KZ_IBAN_RE = re.compile(r"\bKZ[A-Z0-9]{8,32}\b", re.IGNORECASE)
_IIN_RE = re.compile(r"\b\d{12}\b")
_HALYK_CONTRACT_RE = re.compile(r"\bCONTRACT\s*#?\s*(KZ[0-9A-Z]{10,40})\b", re.IGNORECASE)
_CURRENCY_RE = re.compile(r"\b(KZT|USD|EUR|RUB|CNY)\b", re.IGNORECASE)

# FIX 3: explicit pattern for Kaspi totals row
# Matches: "2 дебет" and "0 кредит" as counts/amounts
_KASPI_DEBIT_COUNT_RE  = re.compile(r"(\d[\d\s,.]*)\s*дебет",  re.IGNORECASE)
_KASPI_CREDIT_COUNT_RE = re.compile(r"(\d[\d\s,.]*)\s*кредит", re.IGNORECASE)
# Also matches "Дебет 952.28" and "Кредит 0.00" in the same row
_KASPI_DEBIT_AMT_RE  = re.compile(r"дебет\s+([\d\s,.]+)",  re.IGNORECASE)
_KASPI_CREDIT_AMT_RE = re.compile(r"кредит\s+([\d\s,.]+)", re.IGNORECASE)

FOOTER_PRIMARY = [
    "итого", "итого оборотов", "итого по дебету", "итого по кредиту",
    "оборот по дебету", "оборот по кредиту",
    "исходящий остаток", "входящий остаток",
]

HEADER_STOP_MARKERS = [
    "дата и время операции", "дата операции",
    "дата операции время", "дата операции / время", "дата/время",
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
    return bool(joined) and any(m in joined for m in HEADER_STOP_MARKERS)


def _first_number_to_right(row: List[Any], needle: str, max_look: int = 12) -> Optional[float]:
    nrow = [norm_text(c) for c in row]
    for i, t in enumerate(nrow):
        if not t:
            continue
        if needle in t:
            for j in range(i + 1, min(len(row), i + 1 + max_look)):
                v = parse_decimal(row[j])
                if v is not None:
                    return float(v)
    return None


def _all_numbers(row: List[Any], max_cols: int = 40) -> List[float]:
    vals: List[float] = []
    for c in row[:max_cols]:
        if c is None:
            continue
        v = parse_decimal(c)
        if v is not None:
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


def _clean_text_value(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if s in {"", "'", '"', "-", "—", "–", "''", '""'}:
        return None
    return s


def _strip_label(label: str) -> str:
    """Remove trailing colon that norm_text preserves, e.g. 'клиент:' → 'клиент'."""
    return label.rstrip(":")


# FIX 3 ──────────────────────────────────────────────────────────────────────
def _parse_kaspi_totals_row(row: List[Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    Parse the Kaspi footer row:
      "Итого оборотов в вал | 2 дебет | 0 кредит | ИТОГО | Дебет 952.28 | Кредит 0.00"

    Strategy:
      1. Look for "Дебет <amount>" and "Кредит <amount>" — these are the
         actual monetary totals.
      2. If not found, look for "<N> дебет" and "<N> кредит" patterns and
         discard counts that look like small integers (i.e., transaction counts).
      3. Fall back to two largest numbers only if nothing else works.

    Returns (total_debit, total_credit).
    """
    full_text = _row_text(row, max_cols=40)

    # Priority 1: "Дебет 952.28" / "Кредит 0.00" — explicit labeled amounts
    d_amt_m = _KASPI_DEBIT_AMT_RE.search(full_text)
    c_amt_m = _KASPI_CREDIT_AMT_RE.search(full_text)
    if d_amt_m or c_amt_m:
        total_debit  = parse_decimal(d_amt_m.group(1)) if d_amt_m else None
        total_credit = parse_decimal(c_amt_m.group(1)) if c_amt_m else None
        return total_debit, total_credit

    # Priority 2: "N дебет" / "N кредит" — but filter out transaction counts
    # (small integers like "2 дебет" are counts, not amounts)
    # If the matched value is a whole integer <= 9999, skip it as a count.
    def _extract_money_from_pattern(m: Optional[re.Match]) -> Optional[float]:
        if m is None:
            return None
        v = parse_decimal(m.group(1))
        if v is None:
            return None
        # Reject obvious transaction counts (integer, small)
        if v == int(v) and abs(v) <= 9_999:
            return None
        return float(v)

    d_cnt_m = _KASPI_DEBIT_COUNT_RE.search(full_text)
    c_cnt_m = _KASPI_CREDIT_COUNT_RE.search(full_text)
    total_debit  = _extract_money_from_pattern(d_cnt_m)
    total_credit = _extract_money_from_pattern(c_cnt_m)

    if total_debit is not None or total_credit is not None:
        return total_debit, total_credit

    # Priority 3: fallback — two largest numbers
    nums = _all_numbers(row)
    money = [n for n in nums if abs(n) > 0]
    if len(money) >= 2:
        money_sorted = sorted(money, key=lambda x: abs(x), reverse=True)
        return float(money_sorted[0]), float(money_sorted[1])
    if len(money) == 1:
        return float(money[0]), None

    return None, None
# ─────────────────────────────────────────────────────────────────────────────


class StatementMetadataExtractor:
    def _detect_iban(self, text: str) -> Optional[str]:
        if "contract" in text.lower():
            return None
        s = re.sub(r"\s+", "", text.upper())
        m = _KZ_IBAN_RE.search(s)
        if not m:
            return None
        iban = m.group(0)
        return iban if len(iban) >= 20 else None

    def _detect_iin(self, text: str) -> Optional[str]:
        m = _IIN_RE.search(text)
        return m.group(0) if m else None

    def _detect_contract(self, text: str) -> Optional[str]:
        m = _HALYK_CONTRACT_RE.search(text)
        return m.group(1) if m else None

    def _detect_currency(self, text: str) -> Optional[str]:
        m = _CURRENCY_RE.search(str(text).upper())
        return m.group(1).upper() if m else None

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
        window_tail = grid[tail_start : block.data_end_row_idx + 1]

        raw_pairs: Dict[str, Any] = {}
        raw_lines: List[str] = []
        best: Dict[str, Tuple[Any, float]] = {}

        def push_pair(k: str, v: Any):
            kk = norm_text(k)
            vv = _clean_text_value(v)
            if not kk or vv is None:
                return
            raw_pairs[kk] = vv

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

        if source_bank == "kaspi":
            for i, row in enumerate(window_above):
                cells = row[:10]
                txt = _row_text(cells)
                raw_lines.append(txt)

                c0 = _clean_text_value(cells[0] if len(cells) > 0 else None)
                c2 = _clean_text_value(cells[2] if len(cells) > 2 else None)
                c1 = _clean_text_value(cells[1] if len(cells) > 1 else None)

                # FIX 4 & 5: strip trailing colon before label comparisons
                label = _strip_label(norm_text(c0))

                next_row = window_above[i + 1] if i + 1 < len(window_above) else []
                next_val = _clean_text_value(next_row[0] if next_row else None)
                value = c2 or c1 or next_val

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

                elif "тип счета" in label:  # FIX 5: now reachable after colon strip
                    # push_pair key must match META_KEY_PATTERNS["account_type"] = ["тип счета", ...]
                    push_pair("тип счета", value)

                elif "дата формирования" in label or "дата выписки" in label:
                    push_pair("statement_date", value)

                elif "входящий остаток" in label:
                    push_pair("opening_balance", value)

                elif "исходящий остаток" in label:
                    push_pair("closing_balance", value)

                iban = self._detect_iban(txt)
                if iban and "iban" not in raw_pairs:
                    push_pair("iban", iban)

                detected_iin = self._detect_iin(txt)
                if detected_iin and "иин/бин" not in raw_pairs:
                    push_pair("иин/бин", detected_iin)

        elif source_bank == "halyk":
            for row in window_above:
                cells = row[:14]
                txt = _row_text(cells)
                raw_lines.append(txt)

                c1 = _clean_text_value(cells[1] if len(cells) > 1 else None)

                for c in cells:
                    if c is None:
                        continue
                    s = str(c).strip()
                    if ":" in s:
                        k, v = s.split(":", 1)
                        push_pair(k.strip(), v.strip())

                if txt:
                    detected_iin = self._detect_iin(txt)
                    if detected_iin and (
                        "иин/бин" in txt.lower() or "иин" in txt.lower() or "бин" in txt.lower()
                    ):
                        push_pair("иин/бин", detected_iin)
                        if c1:
                            push_pair("клиент", c1)

                contract = self._detect_contract(txt)
                if contract:
                    push_pair("contract #", contract)

                if "валюта контракта" in txt.lower():
                    cur = self._detect_currency(txt)
                    if cur:
                        push_pair("валюта контракта", cur)

                iban = self._detect_iban(txt)
                if iban and "iban" not in raw_pairs:
                    push_pair("iban", iban)

                for j in range(len(cells)):
                    c_label = cells[j]
                    if c_label is None:
                        continue
                    s0 = str(c_label).strip()
                    if not s0.endswith(":"):
                        continue
                    for k in range(j + 1, min(len(cells), j + 6)):
                        cv = cells[k]
                        sv = _clean_text_value(cv)
                        if sv:
                            push_pair(s0.strip(" :"), sv)
                            break

        else:
            for row in window_above:
                cells = row[:14]
                raw_lines.append(_row_text(cells))

                for c in cells:
                    if c is None:
                        continue
                    s = str(c).strip()
                    if ":" in s:
                        k, v = s.split(":", 1)
                        push_pair(k.strip(), v.strip())

                for j in range(len(cells)):
                    c0 = cells[j]
                    if c0 is None:
                        continue
                    s0 = str(c0).strip()
                    if not s0.endswith(":"):
                        continue
                    for k in range(j + 1, min(len(cells), j + 6)):
                        cv = cells[k]
                        sv = _clean_text_value(cv)
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

        def parse_footer_rows(rows: List[List[Any]]):
            for row in rows:
                joined = _norm_join(row)
                if not joined:
                    continue

                raw_lines.append(_row_text(row))

                if not any(k in joined for k in FOOTER_PRIMARY):
                    continue

                if source_bank == "kaspi":
                    if "исходящий остаток" in joined:
                        nums = _all_numbers(row)
                        if nums:
                            set_best_money("closing_balance", max(nums, key=abs))

                    if "входящий остаток" in joined:
                        nums = _all_numbers(row)
                        if nums:
                            set_best_money("opening_balance", max(nums, key=abs))

                    # FIX 3: use dedicated parser instead of picking two largest numbers
                    if "итого" in joined:
                        d, c = _parse_kaspi_totals_row(row)
                        if d is not None:
                            set_best_money("total_debit", d)
                        if c is not None:
                            set_best_money("total_credit", c)
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
                "header_row_idx": block.header_row_idx,
                "data_start_row_idx": block.data_start_row_idx,
                "data_end_row_idx": block.data_end_row_idx,
                "header_rows": block.header_rows,
            },
        }

        v_client = find_value(META_KEY_PATTERNS["client"])
        if v_client:
            stmt["client_name"] = str(v_client).strip()

        stmt["client_iin_bin"] = looks_like_iin_bin(find_value(META_KEY_PATTERNS["iin_bin"]))

        contract_val = find_value(["contract #"] + META_KEY_PATTERNS["contract"])
        if source_bank == "halyk" and contract_val:
            stmt["account_iban"] = contract_val
        else:
            v_acc = find_value(META_KEY_PATTERNS["account"]) or find_value(["iban"])
            stmt["account_iban"] = looks_like_iban(v_acc)

        v_cur = find_value(META_KEY_PATTERNS["currency"]) or raw_pairs.get("валюта контракта")
        if v_cur:
            stmt["currency"] = str(v_cur).upper()

        stmt["account_type"]   = find_value(META_KEY_PATTERNS["account_type"])
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