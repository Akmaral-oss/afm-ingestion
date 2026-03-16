from __future__ import annotations

# ─────────────────────────────────────────────────────────────────────────────
# The ONE view the LLM is allowed to query.
# Everything else (raw tables, joins) is hidden behind this surface.
# ─────────────────────────────────────────────────────────────────────────────

NL_VIEW = "afm.transactions_nl_view"

# All columns the LLM may reference.
# The validator uses this whitelist.
ALLOWED_COLUMNS: list[str] = [
    # ── transaction ──────────────────────────────────────────────────────────
    "tx_id",
    "source_bank",
    "operation_ts",
    "operation_date",
    "currency",
    "amount_currency",
    "amount_kzt",
    "amount_credit",
    "amount_debit",
    "direction",
    # ── description ──────────────────────────────────────────────────────────
    "operation_type_raw",
    "sdp_name",
    "purpose_code",
    "purpose_text",
    "raw_note",
    # ── payer ────────────────────────────────────────────────────────────────
    "payer_name",
    "payer_iin_bin",
    "payer_residency",
    "payer_bank",
    "payer_account",
    # ── receiver ─────────────────────────────────────────────────────────────
    "receiver_name",
    "receiver_iin_bin",
    "receiver_residency",
    "receiver_bank",
    "receiver_account",
    # ── statement ────────────────────────────────────────────────────────────
    "client_name",
    "client_iin_bin",
    "account_iban",
    "account_type",
    "statement_date",
    "period_from",
    "period_to",
    "opening_balance",
    "closing_balance",
    "total_debit",
    "total_credit",
    # ── semantic ─────────────────────────────────────────────────────────────
    "semantic_text",
    "semantic_embedding",
]

# Human-readable column descriptions used in the prompt.
COLUMN_DESCRIPTIONS: dict[str, str] = {
    "operation_date":     "Дата операции",
    "operation_ts":       "Дата и время операции",
    "amount_kzt":         "Сумма операции в тенге",
    "amount_currency":    "Сумма в валюте проведения",
    "amount_credit":      "Кредитовая сумма",
    "amount_debit":       "Дебетовая сумма",
    "direction":          "Направление: credit (входящее) / debit (исходящее)",
    "currency":           "Валюта операции (KZT, USD, RUB…)",
    "source_bank":        "Банк-источник выписки (kaspi, halyk…)",
    "operation_type_raw": "Вид операции / категория документа",
    "sdp_name":           "Наименование СДП / платёжной системы",
    "purpose_text":       "Назначение платежа",
    "raw_note":           "Примечание из выписки",
    "payer_name":         "Наименование / ФИО плательщика",
    "payer_iin_bin":      "ИИН/БИН плательщика (12 цифр)",
    "payer_residency":    "Резидентство плательщика",
    "payer_bank":         "Банк плательщика",
    "payer_account":      "Счёт / IBAN плательщика",
    "receiver_name":      "Наименование / ФИО получателя",
    "receiver_iin_bin":   "ИИН/БИН получателя (12 цифр)",
    "receiver_residency": "Резидентство получателя",
    "receiver_bank":      "Банк получателя",
    "receiver_account":   "Счёт / IBAN получателя",
    "client_name":        "Клиент (владелец выписки)",
    "client_iin_bin":     "ИИН/БИН клиента",
    "account_iban":       "IBAN счёта выписки",
    "statement_date":     "Дата формирования выписки",
    "period_from":        "Начало периода выписки",
    "period_to":          "Конец периода выписки",
    "opening_balance":    "Входящий остаток",
    "closing_balance":    "Исходящий остаток",
    "total_debit":        "Итого по дебету за период",
    "total_credit":       "Итого по кредиту за период",
    "semantic_text":      "Объединённый семантический текст операции",
    "semantic_embedding": "Вектор семантического поиска",
}


def schema_prompt_block() -> str:
    """Returns the static schema block injected into every LLM prompt."""
    lines = [f"View: {NL_VIEW}", "", "Columns:"]
    for col in ALLOWED_COLUMNS:
        desc = COLUMN_DESCRIPTIONS.get(col, "")
        suffix = f"  — {desc}" if desc else ""
        lines.append(f"  {col}{suffix}")
    return "\n".join(lines)
