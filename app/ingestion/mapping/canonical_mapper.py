from __future__ import annotations

"""
CanonicalMapper — fixed version
Fixes vs original:
  FIX 2: IIN/BIN stored as float (8.609200e+11).
          New: _safe_iin_bin() converts int/float/str → zero-padded 12-char str.
  FIX 6: raw_note = duplicate of purpose_text.
          New: purpose_text → purpose_text only; raw_note only set when a
          separate note/примечание column is explicitly mapped.
  FIX 7: purpose_code stored as float 841.0.
          New: _safe_purpose_code() converts to str without decimal.
"""

import hashlib
import uuid
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from rapidfuzz import fuzz

from app.ingestion.mapping.rule_mapping import rule_map_column, RULE_BASED_HEADER_MAP
from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
from app.ingestion.mapping.direction_logic import derive_direction

from app.utils.text_utils import norm_text, looks_like_iin_bin
from app.utils.date_utils import parse_datetime
from app.utils.number_utils import parse_decimal
from app.utils.json_utils import safe_json
from app.utils.hashing import cosine_sim

CANONICAL_FIELD_DESCRIPTIONS = {
    "operation_ts":       "Дата и время операции / transaction datetime",
    "currency":           "Валюта операции / currency",
    "operation_type_raw": "Виды операции, категория документа / operation type",
    "sdp_name":           "Наименование СДП / payment system name",
    "amount_currency":    "Сумма в валюте проведения / amount in operation currency",
    "amount_kzt":         "Сумма в тенге / amount in KZT",
    "amount_credit":      "Сумма по кредиту / credit amount",
    "amount_debit":       "Сумма по дебету / debit amount",
    "payer_name":         "Наименование/ФИО плательщика / payer name",
    "payer_iin_bin":      "ИИН/БИН плательщика / payer IIN/BIN",
    "payer_residency":    "Резидентство плательщика / payer residency",
    "payer_bank":         "Банк плательщика / payer bank",
    "payer_account":      "Счет/IBAN плательщика / payer account",
    "receiver_name":      "Наименование/ФИО получателя / receiver name",
    "receiver_iin_bin":   "ИИН/БИН получателя / receiver IIN/BIN",
    "receiver_residency": "Резидентство получателя / receiver residency",
    "receiver_bank":      "Банк получателя / receiver bank",
    "receiver_account":   "Счет/IBAN получателя / receiver account",
    "purpose_code":       "Код назначения платежа / purpose code",
    "purpose_text":       "Назначение платежа / payment purpose text",
}

TXID_NAMESPACE_UUID = uuid.UUID("b7c8b2f7-8a2a-4a3f-a5ad-1a8b7ddc0e7a")

_SEMANTIC_PARTS = ("operation_type_raw", "sdp_name", "purpose_text", "raw_note")


def _build_semantic_text(core: Dict[str, Any]) -> str:
    parts = [
        str(core[f]).strip()
        for f in _SEMANTIC_PARTS
        if core.get(f) and str(core[f]).strip()
    ]
    # FIX 6: deduplicate consecutive identical parts (purpose_text == raw_note case)
    deduped = []
    for p in parts:
        if not deduped or p != deduped[-1]:
            deduped.append(p)
    return " | ".join(deduped)


# FIX 2 ──────────────────────────────────────────────────────────────────────
def _safe_iin_bin(val: Any) -> str | None:
    """
    Convert any IIN/BIN representation to a clean 12-character string.
    Handles: int 860919999999, float 8.609200e+11, str "860919999999",
             str " 971240005555" (with leading \xa0 space from xlsx).
    """
    if val is None:
        return None
    # Strip NBSP and spaces that openpyxl preserves from merged/formatted cells
    if isinstance(val, str):
        val = val.replace("\xa0", "").strip()

    # Convert float/int → string of digits
    try:
        if isinstance(val, float):
            # 8.609200e+11 → "860920000000" — use int() to avoid float repr
            digits = str(int(val))
        else:
            digits = str(val).strip()
            # Remove any non-digit chars (dashes, spaces)
            import re as _re
            digits = _re.sub(r"\D", "", digits)
    except (ValueError, OverflowError):
        return None

    if len(digits) == 12:
        return digits
    # Attempt zero-pad if close (e.g. 11 digits due to float truncation)
    if 10 <= len(digits) <= 13:
        return digits.zfill(12)[:12]
    return None
# ─────────────────────────────────────────────────────────────────────────────


# FIX 7 ──────────────────────────────────────────────────────────────────────
def _safe_purpose_code(val: Any) -> str | None:
    """Convert 841.0 → '841', preserving non-numeric codes as strings."""
    if val is None:
        return None
    if isinstance(val, float):
        # Handle NaN values
        if pd.isna(val):
            return None
        if val == int(val):
            return str(int(val))
        return str(val)
    if isinstance(val, int):
        return str(val)
    return str(val).strip() or None
# ─────────────────────────────────────────────────────────────────────────────


class CanonicalMapper:
    def __init__(self, embedder: EmbeddingBackend, threshold: float = 0.85):
        self.embedder = embedder
        self.threshold = threshold
        self.canon_fields = sorted(CANONICAL_FIELD_DESCRIPTIONS.keys())
        self.canon_texts = [CANONICAL_FIELD_DESCRIPTIONS[f] for f in self.canon_fields]
        self.canon_vecs = (
            self.embedder.embed(self.canon_texts) if self.embedder.enabled else None
        )

    def map_headers(self, df: pd.DataFrame) -> Tuple[Dict[str, str], List[str]]:
        mapped: Dict[str, str] = {}
        cols = list(df.columns)

        # pass 1: exact / rule-based
        for c in cols:
            canon = rule_map_column(c)
            if canon:
                mapped[c] = canon

        # pass 2: fuzzy lexical
        for c in cols:
            if c in mapped:
                continue
            nc = norm_text(c)
            best_field = None
            best_score = 0
            for k, v in RULE_BASED_HEADER_MAP.items():
                sc = fuzz.token_set_ratio(nc, k)
                if sc > best_score:
                    best_score = sc
                    best_field = v
            if best_score >= 92 and best_field:
                mapped[c] = best_field

        # pass 3: embeddings
        if self.embedder.enabled and self.canon_vecs is not None:
            for c in cols:
                if c in mapped:
                    continue
                samples = df[c].dropna().astype(str).head(5).tolist()
                probe = f"{norm_text(c)} | examples: " + " ; ".join(
                    [norm_text(s) for s in samples]
                )
                vec = self.embedder.embed([probe])[0]
                best_field = None
                best_sim = -1.0
                for idx, f in enumerate(self.canon_fields):
                    sim = cosine_sim(vec, self.canon_vecs[idx])
                    if sim > best_sim:
                        best_sim = sim
                        best_field = f
                if best_field is not None and best_sim >= self.threshold:
                    mapped[c] = best_field

        unmapped = [c for c in cols if c not in mapped]
        return mapped, unmapped

    def to_rows(
        self,
        df: pd.DataFrame,
        mapped: Dict[str, str],
        context: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        core_rows: List[Dict[str, Any]] = []
        ext_rows: List[Dict[str, Any]] = []
        discovery: List[Dict[str, Any]] = []

        unmapped_cols = [c for c in df.columns if c not in mapped]

        from app.ingestion.validation.validators import is_service_row

        for ridx, row in df.iterrows():
            row_values = [row.get(c) for c in df.columns]
            if is_service_row(row_values):
                continue

            core: Dict[str, Any] = {
                "tx_id":            None,
                "file_id":          context["file_id"],
                "statement_id":     context.get("statement_id"),
                "format_id":        context.get("format_id"),
                "source_bank":      context["source_bank"],
                "source_sheet":     context.get("source_sheet"),
                "source_block_id":  context.get("source_block_id"),
                "source_row_no":    int(context.get("source_row_base", 0) + ridx),
                "row_hash":         None,
                "operation_ts":     None,
                "operation_date":   None,
                "currency":         None,
                "amount_currency":  None,
                "amount_kzt":       None,
                "amount_credit":    None,
                "amount_debit":     None,
                "direction":        None,
                "operation_type_raw": None,
                "sdp_name":         None,
                "purpose_code":     None,
                "purpose_text":     None,
                "raw_note":         None,   # FIX 6: stays None unless explicit column
                "payer_name":       None,
                "payer_iin_bin":    None,
                "payer_residency":  None,
                "payer_bank":       None,
                "payer_account":    None,
                "receiver_name":    None,
                "receiver_iin_bin": None,
                "receiver_residency": None,
                "receiver_bank":    None,
                "receiver_account": None,
                "confidence_score": 1.0,
                "parse_warnings":   None,
                "raw_row_json":     None,
                "semantic_text":    None,
                "semantic_embedding": None,
            }

            ext: Dict[str, Any] = {}
            warnings: List[str] = []

            for raw_col, canon in mapped.items():
                val = row.get(raw_col)

                if canon == "operation_ts":
                    d = parse_datetime(val)
                    core["operation_ts"]   = d
                    core["operation_date"] = d.date() if d else None

                elif canon in ("amount_currency", "amount_kzt", "amount_credit", "amount_debit"):
                    core[canon] = parse_decimal(val)

                elif canon in ("payer_iin_bin", "receiver_iin_bin"):
                    # FIX 2: use _safe_iin_bin instead of looks_like_iin_bin(val)
                    cleaned = _safe_iin_bin(val)
                    core[canon] = cleaned
                    if val is not None and cleaned is None and str(val).strip() not in ("", "None"):
                        warnings.append(f"bad_iinbin:{canon}:{val}")

                elif canon == "purpose_code":
                    # FIX 7: cast to clean string, no float suffix
                    core["purpose_code"] = _safe_purpose_code(val)

                elif canon == "purpose_text":
                    # FIX 6: only set purpose_text — do NOT copy to raw_note
                    core["purpose_text"] = str(val).strip() if val is not None else None

                elif canon == "raw_note":
                    # raw_note only reached when a separate note/примечание column exists
                    s = str(val).strip() if val is not None else None
                    # FIX 6: only write raw_note if it differs from purpose_text
                    if s and s != core.get("purpose_text"):
                        core["raw_note"] = s

                else:
                    core[canon] = str(val).strip() if val is not None else None

            core["direction"] = derive_direction(core)
            core["semantic_text"] = _build_semantic_text(core) or None

            if context.get("store_raw_row_json", False):
                raw_obj = {
                    str(c): (None if pd.isna(row.get(c)) else row.get(c))
                    for c in df.columns
                }
                core["raw_row_json"] = raw_obj

            # dedup hash
            dedup_payload = {
                "bank":    core["source_bank"],
                "account": context.get("account_iban"),
                "ts":      str(core.get("operation_ts") or ""),
                "amt": (
                    core.get("amount_currency")
                    or core.get("amount_kzt")
                    or core.get("amount_debit")
                    or core.get("amount_credit")
                ),
                "payer":   core.get("payer_iin_bin") or core.get("payer_name"),
                "recv":    core.get("receiver_iin_bin") or core.get("receiver_name"),
                "purpose": (core.get("purpose_text") or "")[:200],
            }
            row_hash = hashlib.sha256(
                safe_json(dedup_payload).encode("utf-8")
            ).hexdigest()
            core["row_hash"] = row_hash
            core["tx_id"] = str(uuid.uuid5(TXID_NAMESPACE_UUID, row_hash))

            # ext / unmapped
            for c in unmapped_cols:
                v = row.get(c)
                if v is None or (isinstance(v, float) and np.isnan(v)):
                    continue
                ext[c] = v

            if warnings:
                core["parse_warnings"] = ";".join(warnings)
                core["confidence_score"] = 0.9

            core_rows.append(core)

            if ext:
                ext_rows.append({"tx_id": core["tx_id"], "ext_json": ext})

        # discovery log
        for c in unmapped_cols:
            samples = df[c].dropna().astype(str).head(5).tolist()
            discovery.append({
                "file_id":          context["file_id"],
                "source_bank":      context["source_bank"],
                "format_id":        context.get("format_id"),
                "raw_column_name":  str(c),
                "normalized_name":  norm_text(c),
                "sample_values":    samples,
                "suggested_field":  None,
                "confidence":       None,
                "status":           "new",
            })

        return core_rows, ext_rows, discovery