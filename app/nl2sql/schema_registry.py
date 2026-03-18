from __future__ import annotations

NL_VIEW = "afm.transactions_nl_view"

_BASE_COLUMNS: list[str] = [
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
    "operation_type_raw",
    "sdp_name",
    "purpose_code",
    "purpose_text",
    "raw_note",
    "payer_name",
    "payer_iin_bin",
    "payer_residency",
    "payer_bank",
    "payer_account",
    "receiver_name",
    "receiver_iin_bin",
    "receiver_residency",
    "receiver_bank",
    "receiver_account",
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
]
_SEMANTIC_COLUMNS: list[str] = [
    "semantic_text",
    "semantic_embedding",
]

ALLOWED_COLUMNS: list[str] = _BASE_COLUMNS + _SEMANTIC_COLUMNS

COLUMN_DESCRIPTIONS: dict[str, str] = {
    "operation_date": "Дата операции",
    "operation_ts": "Дата и время операции",
    "amount_kzt": "Сумма операции в тенге",
    "amount_currency": "Сумма в валюте проведения",
    "amount_credit": "Кредитовая сумма",
    "amount_debit": "Дебетовая сумма",
    "direction": "Направление: credit / debit",
    "currency": "Валюта операции",
    "source_bank": "Банк-источник выписки",
    "operation_type_raw": "Вид операции / категория документа",
    "sdp_name": "Наименование СДП / платёжной системы",
    "purpose_code": "Код назначения платежа",
    "purpose_text": "Назначение платежа",
    "raw_note": "Примечание из выписки",
    "payer_name": "Плательщик",
    "payer_iin_bin": "ИИН/БИН плательщика",
    "payer_residency": "Резидентство плательщика",
    "payer_bank": "Банк плательщика",
    "payer_account": "Счёт / IBAN плательщика",
    "receiver_name": "Получатель",
    "receiver_iin_bin": "ИИН/БИН получателя",
    "receiver_residency": "Резидентство получателя",
    "receiver_bank": "Банк получателя",
    "receiver_account": "Счёт / IBAN получателя",
    "client_name": "Клиент (владелец выписки)",
    "client_iin_bin": "ИИН/БИН клиента",
    "account_iban": "IBAN счёта выписки",
    "account_type": "Тип счёта",
    "statement_date": "Дата формирования выписки",
    "period_from": "Начало периода выписки",
    "period_to": "Конец периода выписки",
    "opening_balance": "Входящий остаток",
    "closing_balance": "Исходящий остаток",
    "total_debit": "Итого по дебету за период",
    "total_credit": "Итого по кредиту за период",
    "semantic_text": "Объединённый семантический текст операции",
    "semantic_embedding": "Вектор семантического поиска",
}


def schema_prompt_block(*, include_semantic: bool = True) -> str:
    columns = list(_BASE_COLUMNS)
    if include_semantic:
        columns.extend(_SEMANTIC_COLUMNS)

    lines = [f"View: {NL_VIEW}", "", "Columns:"]
    for column in columns:
        description = COLUMN_DESCRIPTIONS.get(column, "")
        suffix = f"  — {description}" if description else ""
        lines.append(f"  {column}{suffix}")
    return "\n".join(lines)
