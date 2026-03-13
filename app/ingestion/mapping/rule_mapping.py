from app.utils.text_utils import norm_text

KASPI_COMBINED_TO_CANONICAL = {
    "payer/name": "payer_name",
    "payer/iin_bin": "payer_iin_bin",
    "payer/iin": "payer_iin_bin",
    "payer/bin": "payer_iin_bin",
    "payer/residency": "payer_residency",
    "payer/bank": "payer_bank",
    "payer/account": "payer_account",

    "receiver/name": "receiver_name",
    "receiver/iin_bin": "receiver_iin_bin",
    "receiver/iin": "receiver_iin_bin",
    "receiver/bin": "receiver_iin_bin",
    "receiver/residency": "receiver_residency",
    "receiver/bank": "receiver_bank",
    "receiver/account": "receiver_account",
}

RULE_BASED_HEADER_MAP = {
    "дата и время операции": "operation_ts",
    "дата операции": "operation_ts",
    "дата/время": "operation_ts",
    "дата": "operation_ts",

    "валюта операции": "currency",
    "валюта": "currency",

    "виды операции (категория документа)": "operation_type_raw",
    "виды операции": "operation_type_raw",
    "категория документа": "operation_type_raw",

    "наименование сдп (при наличии)": "sdp_name",
    "наименование сдп": "sdp_name",
    "сдп": "sdp_name",

    "код назначения платежа": "purpose_code",
    "назначение платежа": "purpose_text",
    "назначение": "purpose_text",

    "сумма в тенге": "amount_kzt",
    "сумма в валюте ее проведения": "amount_currency",
    "сумма в валюте её проведения": "amount_currency",
    "сумма в валюте ее проведения по кредиту": "amount_credit",
    "сумма в валюте ее проведения по дебету": "amount_debit",
    "сумма по кредиту": "amount_credit",
    "сумма по дебету": "amount_debit",
    "сумма": "amount_currency",

    "наименование/фио плательщика": "payer_name",
    "иин/бин плательщика": "payer_iin_bin",
    "резидентство плательщика": "payer_residency",
    "банк плательщика": "payer_bank",
    "счет плательщика": "payer_account",
    "номер счета плательщика": "payer_account",

    "наименование/фио получателя": "receiver_name",
    "иин/бин получателя": "receiver_iin_bin",
    "резидентство получателя": "receiver_residency",
    "банк получателя": "receiver_bank",
    "счет получателя": "receiver_account",
    "номер счета получателя": "receiver_account",
}


def rule_map_column(col: str):
    nc = norm_text(col)
    if nc in RULE_BASED_HEADER_MAP:
        return RULE_BASED_HEADER_MAP[nc]
    if nc in KASPI_COMBINED_TO_CANONICAL:
        return KASPI_COMBINED_TO_CANONICAL[nc]
    return None