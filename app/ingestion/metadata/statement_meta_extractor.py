from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from app.ingestion.extractor.block_detector import DetectedBlock
from app.ingestion.metadata.meta_patterns import META_KEY_PATTERNS
from app.utils.text_utils import norm_text, looks_like_iban, looks_like_iin_bin
from app.utils.date_utils import parse_date, parse_period
from app.utils.number_utils import parse_decimal


_KZ_IBAN_RE = re.compile(r"\bKZ[A-Z0-9]{8,32}\b", re.IGNORECASE)

FOOTER_PRIMARY = ["итого", "итого оборотов", "исходящий остаток", "входящий остаток"]


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


def _first_number_to_right(
    row: List[Any],
    needle: str,
    max_look: int = 12,
) -> Optional[float]:
    nrow = [norm_text(c) for c in row]
    for i, t in enumerate(nrow):
        if not t:
            continue
        if needle in t:
            for j in range(i + 1, min(len(row), i + 1 + max_look)):
                v = parse_decimal(row[j])
                if v is None:
                    continue
                try:
                    return float(v)
                except Exception:
                    continue
    return None


def _all_numbers(row: List[Any], max_cols: int = 40) -> List[float]:
    vals: List[float] = []
    for c in row[:max_cols]:
        if c is None:
            continue
        v = parse_decimal(c)
        if v is None:
            continue
        try:
            vals.append(float(v))
        except Exception:
            continue
    return vals


def _money_score(x: float) -> float:
    """
    Heuristic: big amounts should win.
    525/298 should lose vs 34,926,795.38
    """
    ax = abs(x)
    if ax >= 1_000_000:
        return ax + 10_000_000
    if ax >= 10_000:
        return ax + 1_000_000
    if ax >= 1_000:
        return ax + 100_000
    # tiny numbers (counts) are low score
    return ax


class StatementMetadataExtractor:
    def extract_for_block(
        self,
        grid: List[List[Any]],
        block: DetectedBlock,
        source_bank: str,
        max_lookback_rows: int = 80,
        max_lookahead_rows: int = 60,
        tail_rows_in_block: int = 80,
    ) -> Dict[str, Any]:
        header_i = block.header_row_idx
        start = max(0, header_i - max_lookback_rows)
        window_above = grid[start:header_i]

        # rows after table
        footer_start = min(len(grid), block.data_end_row_idx + 1)
        footer_end = min(len(grid), footer_start + max_lookahead_rows)
        window_below = grid[footer_start:footer_end]

        # last rows inside table (Kaspi totals are often here)
        tail_start = max(block.data_start_row_idx, block.data_end_row_idx - tail_rows_in_block)
        window_tail = grid[tail_start : block.data_end_row_idx + 1]

        raw_pairs: Dict[str, Any] = {}
        raw_lines: List[str] = []

        # keep "best" candidates for money fields
        best: Dict[str, Tuple[Any, float]] = {}

        def push_pair_once(k: str, v: Any):
            kk = norm_text(k)
            if not kk:
                return
            if kk in raw_pairs:
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
            try:
                fv = float(parse_decimal(v)) if parse_decimal(v) is not None else float(v)
            except Exception:
                return
            score = _money_score(fv)
            if kk not in best or score > best[kk][1]:
                best[kk] = (v, score)

        # ----------------------------
        # ABOVE HEADER parsing (client/iban/period etc.)
        # ----------------------------
        for row in window_above:
            cells = [c for c in row[:14]]
            txts = [str(c) for c in cells if c is not None and str(c).strip() != ""]
            if not txts:
                continue

            raw_lines.append(_row_text(cells, max_cols=14))

            # key:value in one cell
            for c in cells:
                if c is None:
                    continue
                s = str(c).strip()
                if ":" in s:
                    k, v = s.split(":", 1)
                    k, v = k.strip(), v.strip()
                    if k:
                        push_pair_once(k, v)

            # label: -> first non-empty cell to the right
            for j in range(min(12, len(cells))):
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
                        push_pair_once(s0.strip(" :"), sv)
                        break

            # iin/bin
            for c in cells:
                if c is None:
                    continue
                s = str(c)
                m = re.search(r"(иин/бин|бин/иин)\s*([0-9]{12})", s.lower())
                if m:
                    push_pair_once("иин/бин", m.group(2))

            # IBAN
            for c in cells:
                if c is None:
                    continue
                s = re.sub(r"\s+", "", str(c).upper())
                m = _KZ_IBAN_RE.search(s)
                if m:
                    push_pair_once("iban", m.group(0).upper())

        # ----------------------------
        # FOOTER parsing (tail + below)
        # ----------------------------
        def parse_footer_rows(rows: List[List[Any]]):
            for row in rows:
                joined = _norm_join(row, max_cols=32)
                if not joined:
                    continue

                # keep for debugging
                raw_lines.append(_row_text(row, max_cols=32))

                if not any(k in joined for k in FOOTER_PRIMARY):
                    continue

                # balances
                if "исходящий остаток" in joined:
                    v = _first_number_to_right(row, "исходящий остаток")
                    if v is None:
                        nums = _all_numbers(row)
                        v = max(nums) if nums else None
                    if v is not None:
                        set_best_money("исходящий остаток", v)

                if "входящий остаток" in joined:
                    v = _first_number_to_right(row, "входящий остаток")
                    if v is None:
                        nums = _all_numbers(row)
                        v = max(nums) if nums else None
                    if v is not None:
                        set_best_money("входящий остаток", v)

                # IMPORTANT:
                # "итого оборотов ..." has counts (525/298). We must NOT treat it as money totals.
                if "итого оборотов" in joined:
                    continue

                # money totals: look for "итого" + debit/credit
                if "итого" in joined:
                    d = _first_number_to_right(row, "дебет")
                    c = _first_number_to_right(row, "кредит")

                    # fallback: if row contains BOTH words and has many numbers,
                    # pick two largest numbers as money-like candidates
                    if (d is None) or (c is None):
                        nums = _all_numbers(row)
                        if nums and len(nums) >= 2:
                            top2 = sorted(nums, key=lambda x: abs(x), reverse=True)[:2]
                            # assign by keyword presence if missing
                            if d is None and "дебет" in joined:
                                d = top2[0]
                            if c is None and "кредит" in joined:
                                c = top2[1] if len(top2) > 1 else None

                    if d is not None:
                        set_best_money("итого дебет", d)
                    if c is not None:
                        set_best_money("итого кредит", c)

        parse_footer_rows(window_tail)
        parse_footer_rows(window_below)

        # move best candidates into raw_pairs (so existing META_KEY_PATTERNS logic works)
        for k, (v, _score) in best.items():
            # store normalized keys same as earlier style
            raw_pairs[norm_text(k)] = v

        # ----------------------------
        # lookup helper
        # ----------------------------
        def find_value(keys: List[str]) -> Optional[Any]:
            for rk, rv in raw_pairs.items():
                for kk in keys:
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
                "raw_lines": raw_lines[:400],
                "source_bank_hint": source_bank,
            },
        }

        v_client = find_value(META_KEY_PATTERNS["client"])
        if v_client and not looks_like_iban(v_client) and not looks_like_iin_bin(v_client):
            stmt["client_name"] = str(v_client).strip()

        v_iin = find_value(META_KEY_PATTERNS["iin_bin"])
        stmt["client_iin_bin"] = looks_like_iin_bin(v_iin)

        v_contract = find_value(META_KEY_PATTERNS["contract"])
        if v_contract:
            stmt["contract_no"] = str(v_contract).strip()

        v_acc = find_value(META_KEY_PATTERNS["account"]) or find_value(["iban"])
        iban = looks_like_iban(v_acc)
        if iban:
            stmt["account_iban"] = iban
        else:
            for line in raw_lines:
                m = _KZ_IBAN_RE.search(line.replace(" ", "").upper())
                if m:
                    stmt["account_iban"] = m.group(0).upper()
                    break

        v_cur = find_value(META_KEY_PATTERNS["currency"])
        if v_cur:
            s = str(v_cur).strip().upper()
            m = re.search(r"\b(KZT|USD|EUR|RUB|CNY)\b", s)
            if m:
                stmt["currency"] = m.group(1)

        v_at = find_value(META_KEY_PATTERNS["account_type"])
        if v_at:
            stmt["account_type"] = str(v_at).strip()

        v_sd = find_value(META_KEY_PATTERNS["statement_date"])
        if v_sd:
            stmt["statement_date"] = parse_date(v_sd)

        v_per = find_value(META_KEY_PATTERNS["period"])
        if v_per:
            d1, d2 = parse_period(v_per)
            stmt["period_from"], stmt["period_to"] = d1, d2

        v_ob = find_value(META_KEY_PATTERNS["opening_balance"]) or find_value(["входящий остаток"])
        if v_ob is not None:
            stmt["opening_balance"] = parse_decimal(v_ob)

        v_cb = find_value(META_KEY_PATTERNS["closing_balance"]) or find_value(["исходящий остаток"])
        if v_cb is not None:
            stmt["closing_balance"] = parse_decimal(v_cb)

        v_td = find_value(META_KEY_PATTERNS["total_debit"]) or find_value(["итого дебет"])
        if v_td is not None:
            stmt["total_debit"] = parse_decimal(v_td)

        v_tc = find_value(META_KEY_PATTERNS["total_credit"]) or find_value(["итого кредит"])
        if v_tc is not None:
            stmt["total_credit"] = parse_decimal(v_tc)

        return stmt