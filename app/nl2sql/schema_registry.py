from __future__ import annotations

NL_VIEW = "afm.transactions_nl_view"

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
    # ── statement (уже включён через LEFT JOIN в view) ────────────────────────
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
    # ── classification (колонки прямо в transactions_core) ────────────────────
    "transaction_category",
    "transaction_category_ru",
    "category_group",
    "category_confidence",
    "category_source",
    "needs_review",
    "signed_amount_kzt",
]

COLUMN_DESCRIPTIONS: dict[str, str] = {
    # ── transaction ──────────────────────────────────────────────────────────
    "tx_id":              "UUID транзакции",
    "source_bank":        "Банк-источник выписки. Значения: 'kaspi', 'halyk'",
    "operation_ts":       "Дата и время операции (timestamp)",
    "operation_date":     "Дата операции (date)",
    "currency":           "Валюта операции (KZT, USD, EUR, RUB…)",
    "amount_currency":    "Сумма в валюте проведения",
    "amount_kzt":         "Сумма в тенге. Часто NULL — всегда фильтруй WHERE amount_kzt IS NOT NULL в агрегациях",
    "amount_credit":      "Кредитовая сумма (входящая). Альтернатива direction='credit'",
    "amount_debit":       "Дебетовая сумма (исходящая). Альтернатива direction='debit'",
    "direction": (
        "Направление: 'credit'=входящее / 'debit'=исходящее / NULL=не определено. "
        "НЕНАДЁЖНО — часто NULL у Halyk. "
        "Используй вместе с operation_type_raw: "
        "ИСХ/исх.doc.(дебет)=дебет; ВХ/вх.doc.(кредит)=кредит"
    ),
    # ── description ──────────────────────────────────────────────────────────
    "operation_type_raw": (
        "Вид операции из выписки: "
        "исх.doc.(дебет)/ИСХ=исходящий; "
        "вх.doc.(кредит)/ВХ=входящий; "
        "CH Debit/ATM=банкомат; SOA_CREDIT=пополнение"
    ),
    "sdp_name":           "СДП / платёжная система (Kaspi Pay, Red.kz…)",
    "purpose_code":       "КНП — код назначения платежа. 119=P2P, 191=card-to-card, 390=внутренние, 411=выдача займа, 421=погашение займа, 851=услуги, 880=возврат, 911=налоги",
    "purpose_text":       "Назначение платежа — основное текстовое поле для LIKE-поиска",
    "raw_note":           "Примечание из выписки (может дублировать purpose_text)",
    # ── payer ────────────────────────────────────────────────────────────────
    "payer_name":         "ФИО / наименование плательщика",
    "payer_iin_bin":      "ИИН/БИН плательщика (12 цифр). 5-я цифра: 4/5=ЮЛ, 6=ИП, 1-3=ФЛ",
    "payer_residency":    "Резидентство плательщика",
    "payer_bank":         "Банк плательщика (свободный текст)",
    "payer_account":      "IBAN / счёт плательщика",
    # ── receiver ─────────────────────────────────────────────────────────────
    "receiver_name":      "ФИО / наименование получателя",
    "receiver_iin_bin":   "ИИН/БИН получателя (12 цифр). 5-я цифра: 4/5=ЮЛ, 6=ИП, 1-3=ФЛ",
    "receiver_residency": "Резидентство получателя",
    "receiver_bank":      "Банк получателя (свободный текст)",
    "receiver_account":   "IBAN / счёт получателя",
    # ── statement ────────────────────────────────────────────────────────────
    "client_name":        "Владелец выписки (уже в view, JOIN не нужен)",
    "client_iin_bin":     "ИИН/БИН владельца выписки",
    "account_iban":       "IBAN счёта выписки",
    "account_type":       "Тип счёта (Текущий, Депозитный…)",
    "statement_date":     "Дата формирования выписки",
    "period_from":        "Начало периода выписки",
    "period_to":          "Конец периода выписки",
    "opening_balance":    "Входящий остаток на начало периода",
    "closing_balance":    "Исходящий остаток на конец периода",
    "total_debit":        "Итого дебет за период (из шапки выписки)",
    "total_credit":       "Итого кредит за период (из шапки выписки)",
    # ── semantic ─────────────────────────────────────────────────────────────
    "semantic_text":      "Очищенный текст для семантического поиска",
    "semantic_embedding": "Вектор BGE-M3. ТОЛЬКО для ORDER BY semantic_embedding <-> :query_embedding",
    # ── classification ────────────────────────────────────────────────────────
    "transaction_category": (
        "Бизнес-категория операции — колонка в transactions_core, назначается при ingestion. "
        "Точные коды: "
        "P2P_TRANSFER / STORE_PURCHASE / INTERNAL_OPERATION / CASH_WITHDRAWAL / "
        "LOAN_REPAYMENT / GAMBLING / MANDATORY_PAYMENT / STATE_PAYMENT / SALARY / "
        "ACCOUNT_TOPUP / CONTRACT_SETTLEMENT / INVOICE_PAYMENT / CARD_PAYMENT / "
        "FX_OPERATION / LOAN_ISSUANCE / ALIMONY / SECURITIES / REFUND / OTHER. "
        "Используй: WHERE transaction_category = 'ЗАРПЛАТА'"
    ),
    "transaction_category_ru": (
        "Категория на русском для отображения и GROUP BY: "
        "'P2P перевод' / 'Покупка в магазине' / 'Внутренние операции' / 'Снятие наличных' / "
        "'Погашение кредита' / 'Онлайн-игры / Гемблинг' / 'Обязательные платежи' / "
        "'Госвыплата' / 'Зарплата' / 'Пополнение счёта' / 'Расчёты по договору' / "
        "'Оплата по счёт-фактуре' / 'Платёж на карту' / 'Валютная операция' / "
        "'Выдача займа' / 'Алименты' / 'Операции с ценными бумагами' / "
        "'Возврат средств' / 'Прочее'"
    ),
    "category_group": (
        "Группа категорий: Переводы / Платежи / Наличные / Кредиты / Риск / "
        "Обязательные / Поступления / Пополнения / Расчёты / Валюта / "
        "Инвестиции / Корректировки / Прочее. "
        "Используй для группировки по типу: GROUP BY category_group"
    ),
    "category_confidence": (
        "Уверенность классификатора 0.0–1.0. "
        "rule=0.95, embedding=0.35..1.0. "
        "Используй для фильтрации ненадёжных: WHERE category_confidence >= 0.7"
    ),
    "category_source": (
        "Источник классификации: "
        "'rule'=сработало regex-правило (надёжно) / "
        "'embedding'=BGE-M3 cosine similarity (менее надёжно) / "
        "'other'=не определено. "
        "Используй: WHERE category_source = 'rule' для высокой точности"
    ),
    "needs_review": (
        "TRUE если классификатор не уверен (embedding confidence < 0.55 или OTHER). "
        "Используй для фильтрации сомнительных: WHERE needs_review = FALSE"
    ),
    "signed_amount_kzt": (
        "Сумма со знаком: +amount_kzt для direction='credit', -amount_kzt для direction='debit', NULL если direction неизвестен. "
        "ВСЕГДА используй для расчёта баланса вместо CASE WHEN direction. "
        "Пример: SUM(signed_amount_kzt) AS net_balance GROUP BY transaction_category_ru"
    ),
}


def schema_prompt_block() -> str:
    """Статический блок схемы — вставляется в каждый LLM-промпт."""
    lines = [
        f"View: {NL_VIEW}",
        "",
        "IMPORTANT: Этот view уже включает данные из statements через LEFT JOIN.",
        "Поля client_name, account_iban, period_from/to, opening/closing_balance,",
        "total_debit, total_credit — доступны напрямую, JOIN писать НЕ НУЖНО.",
        "",
        "CLASSIFICATION: transaction_category — колонка прямо в transactions_core.",
        "Назначается при ingestion. Для точных фильтров: WHERE transaction_category = '<КОД>'",
        "Для отображения и GROUP BY: transaction_category_ru",
        "Для баланса: signed_amount_kzt (уже со знаком ±)",
        "",
        "Columns:",
    ]
    for col in ALLOWED_COLUMNS:
        desc = COLUMN_DESCRIPTIONS.get(col, "")
        suffix = f"  — {desc}" if desc else ""
        lines.append(f"  {col}{suffix}")
    return "\n".join(lines)
