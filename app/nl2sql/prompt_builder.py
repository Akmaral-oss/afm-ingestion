from __future__ import annotations

"""
Prompt builder.

6-block prompt:
  1. System role
  2. Schema (static)
  3. Query strategy — decision tree + built-in few-shot examples
  4. Extracted entities (dynamic)
  5. Retrieved context — cluster samples + history (dynamic)
  6. User question
"""

from .query_models import QueryPlan
from .schema_registry import NL_VIEW, schema_prompt_block

# ─────────────────────────────────────────────────────────────────────────────
# Block 1 — System role
# ─────────────────────────────────────────────────────────────────────────────

_SYSTEM_ROLE = f"""\
You are a PostgreSQL expert for a financial intelligence platform.
Convert the user's natural language question into one valid SELECT query
against the view {NL_VIEW}.
Output raw SQL only — no markdown, no comments, no explanation.\
"""

# ─────────────────────────────────────────────────────────────────────────────
# Block 3 — Strategy (decision tree + examples) — ULTIMATE PROMPT v2
# ─────────────────────────────────────────────────────────────────────────────

def _build_strategy(embedder_enabled: bool = True) -> str:
    _sem_rule = (
        "- semantic_text is ONLY for ORDER BY semantic_embedding <-> :query_embedding."
        if embedder_enabled else
        "- EMBEDDER IS DISABLED: Do NOT use ORDER BY semantic_embedding <-> :query_embedding"
        " anywhere. No :query_embedding parameter exists. Use ORDER BY operation_date DESC instead."
    )
    return f"""\
## HARD RULES (CRITICAL)

- ONLY ONE SELECT statement is allowed.
- DO NOT use WITH (CTE). Always rewrite using subqueries.
- ONLY use {NL_VIEW}.
  NEVER use:
    afm.transactions_view
    afm.transactions_core
    afm.statements
  If you are about to use another table — STOP and rewrite.

- LIMIT 100 by default. Omit LIMIT only for GROUP BY aggregations.
- Never SELECT semantic_embedding or SELECT *.
- Use operation_date for dates, amount_kzt for amounts.

- ALWAYS filter NULLs:
  Row-level:
    amount_kzt IS NOT NULL AND operation_date IS NOT NULL
  Aggregation:
    amount_kzt IS NOT NULL
  Group keys:
    client_name IS NOT NULL (if grouping by client)

---

## WINDOW FUNCTION RULE (CRITICAL)

When using LAG / LEAD:

1. FIRST aggregate in subquery
2. THEN apply window function outside

WRONG:
  LAG(SUM(amount_kzt)) OVER (...)

CORRECT:
  SELECT ..., LAG(daily_total) OVER (...)
  FROM (
      SELECT ..., SUM(amount_kzt) AS daily_total
      ...
  ) sub

---

## SPIKE / ANOMALY RULE

Words:
  "резко вырос", "скачок", "аномалия", "spike"

Mean:
  current_value > previous_value * N

Where:
  N = 2 or 3 (default 3)

NEVER:
  current_value > previous_value

---

## FRAUD / RISK FEATURE RULE

When ranking or detecting suspicious behavior:

Use behavioral features:

1. Large transactions:
   amount_kzt > threshold

2. Self-transfer:
   payer_name = receiver_name

3. Repeated pairs:
   payer_name + receiver_name repeated

4. Suspicious text:
   use ONLY purpose_text and raw_note

NEVER define behavior using ONLY text LIKE.

---

## SELF-TRANSFER RULE

"перевод самому себе" means:

  payer_name = receiver_name

DO NOT use:
  purpose_text LIKE '%перевод%'

---

## GROUP NULL RULE

When grouping:

  ALWAYS exclude NULL group keys:

  WHERE client_name IS NOT NULL

---

## QUERY TYPE DECISION

1. FILTER → structured only
2. TOPIC → LIKE anchors
3. HYBRID → filters + topic
4. SEMANTIC → embedding
5. AGGREGATION → GROUP BY

---

## CATEGORY FIELD RULES (v4.0)

transaction_category — это ОБЫЧНАЯ КОЛОНКА в каждой строке (не отдельная таблица).
Назначается автоматически при ingestion. Всегда надёжна для rule-классификаций.

### 19 точных кодов (используй в WHERE):

| Код                  | Русское название              | Когда использовать                              |
|----------------------|-------------------------------|------------------------------------------------|
| P2P_TRANSFER         | P2P перевод                   | card to card, переводы между физлицами          |
| STORE_PURCHASE       | Покупка в магазине            | QR PAY, POS, оплата товаров, kaspi.kz          |
| INTERNAL_OPERATION   | Внутренние операции           | перевод между своими счетами, на депозит        |
| CASH_WITHDRAWAL      | Снятие наличных               | банкомат, ATM, получение наличных               |
| LOAN_REPAYMENT       | Погашение кредита             | погашение займа, долга, ипотеки                 |
| GAMBLING             | Онлайн-игры / Гемблинг        | 1xbet, casino, букмекер, ставки                 |
| MANDATORY_PAYMENT    | Обязательные платежи          | КПН, ИПН, ОСМС, налоги, штрафы, комиссии банка |
| STATE_PAYMENT        | Госвыплата                    | пенсия, пособие, ЕНПФ, соцвыплата               |
| SALARY               | Зарплата                      | зп карта, зарплата, оклад, salary               |
| ACCOUNT_TOPUP        | Пополнение счёта              | пополнение карты, cash in                       |
| CONTRACT_SETTLEMENT  | Расчёты по договору           | оплата за услуги по договору                    |
| INVOICE_PAYMENT      | Оплата по счёт-фактуре        | счёт-фактура, invoice                           |
| CARD_PAYMENT         | Платёж на карту               | платёж на карту (не P2P)                        |
| FX_OPERATION         | Валютная операция             | конвертация, обмен валют, forex                 |
| LOAN_ISSUANCE        | Выдача займа                  | выдача займа, микрокредит, рассрочка            |
| ALIMONY              | Алименты                      | алименты                                        |
| SECURITIES           | Операции с ценными бумагами   | акции, облигации, брокер, KASE                  |
| REFUND               | Возврат средств               | возврат, refund, сторно                         |
| OTHER                | Прочее                        | не определено                                   |

### Шаблоны SQL:

**Фильтр по категории:**
```sql
WHERE transaction_category = 'SALARY'
```

**Разбивка по категориям:**
```sql
SELECT transaction_category_ru, COUNT(*) AS tx_count, SUM(amount_kzt) AS total
FROM afm.transactions_nl_view
WHERE amount_kzt IS NOT NULL
GROUP BY transaction_category_ru
ORDER BY total DESC;
```

**Баланс по категориям (обязательно signed_amount_kzt):**
```sql
SELECT transaction_category_ru,
       SUM(signed_amount_kzt) AS net_balance,
       COUNT(*) AS tx_count
FROM afm.transactions_nl_view
WHERE client_name ILIKE '%Иванов%'
  AND signed_amount_kzt IS NOT NULL
GROUP BY transaction_category_ru
ORDER BY net_balance DESC;
```

**Только надёжные классификации:**
```sql
WHERE transaction_category = 'GAMBLING'
  AND category_source = 'rule'
```

**Транзакции требующие проверки:**
```sql
WHERE needs_review = TRUE
ORDER BY operation_date DESC
```

**Группировка по группам категорий:**
```sql
SELECT category_group, SUM(amount_kzt) AS total
FROM afm.transactions_nl_view
WHERE amount_kzt IS NOT NULL
GROUP BY category_group
ORDER BY total DESC;
```

### ОБЯЗАТЕЛЬНЫЕ ПРАВИЛА:

- Когда пользователь говорит "по категории X", "зарплатные операции", "гемблинг" →
  ВСЕГДА используй WHERE transaction_category = '<КОД>', НЕ LIKE по purpose_text
- Для баланса (сколько пришло/ушло) → ВСЕГДА signed_amount_kzt, не CASE WHEN direction
- Для отображения → transaction_category_ru
- Для GROUP BY → можно и transaction_category (код), и transaction_category_ru (русский)
- category_source = 'rule' означает высокую точность (confidence = 0.95)
- category_source = 'embedding' означает меньшую уверенность

## SAFE PATTERNS

### DAILY AGG + LAG (SPIKE)
```sql
SELECT *
FROM (
    SELECT
        client_name,
        operation_date,
        daily_total,
        LAG(daily_total) OVER (
            PARTITION BY client_name
            ORDER BY operation_date
        ) AS prev_day_total
    FROM (
        SELECT
            client_name,
            operation_date,
            SUM(amount_kzt) AS daily_total
        FROM {NL_VIEW}
        WHERE amount_kzt IS NOT NULL
          AND client_name IS NOT NULL
          AND operation_date IS NOT NULL
        GROUP BY client_name, operation_date
    ) d
) t
WHERE prev_day_total IS NOT NULL
  AND daily_total > prev_day_total * 3
ORDER BY daily_total DESC
LIMIT 100;
```

---

### RISK SCORE
```sql
SELECT
    client_name,
    SUM(CASE WHEN amount_kzt > 1000000 THEN 1 ELSE 0 END) AS large_tx,
    SUM(CASE WHEN payer_name = receiver_name THEN 1 ELSE 0 END) AS self_tx,
    COUNT(*) AS total_tx,
    (
        SUM(CASE WHEN amount_kzt > 1000000 THEN 1 ELSE 0 END) * 3 +
        SUM(CASE WHEN payer_name = receiver_name THEN 1 ELSE 0 END) * 5 +
        COUNT(*) * 0.1
    ) AS risk_score
FROM {NL_VIEW}
WHERE amount_kzt IS NOT NULL
  AND client_name IS NOT NULL
GROUP BY client_name
ORDER BY risk_score DESC
LIMIT 20;
```

---

{_sem_rule}

## ADDITIONAL RULES

- A standalone 4-digit year (2024, 2025) is a DATE filter, never an amount.
- NEVER use LIKE on semantic_text. It contains operation_type_raw which includes
  accounting terms "кредит/дебет" — this causes false positives on ALL credit/debit
  transactions.
- For text LIKE searches use ONLY: purpose_text and raw_note.
- DIRECTION is unreliable — often NULL. Always use all three signals together:
  1. direction = 'credit' / 'debit' (may be NULL)
  2. operation_type_raw: ИСХ/исх.doc.(дебет) = debit; ВХ/вх.doc.(кредит) = credit
  3. amount_credit / amount_debit columns
  For "входящие/поступления/кредит":
    (direction = 'credit' OR operation_type_raw ILIKE '%вх%' OR operation_type_raw ILIKE '%кредит%' OR amount_credit > 0)
  For "исходящие/списания/дебет":
    (direction = 'debit' OR direction IS NULL OR operation_type_raw ILIKE '%исх%' OR operation_type_raw ILIKE '%дебет%')
  HALYK SPECIAL: for halyk + debit always add OR direction IS NULL
- BANK FILTERING — three separate columns, use the right one:
  source_bank   = the bank that issued the statement. Values: 'kaspi', 'halyk'.
                  Use for: "kaspi операции", "halyk транзакции"
                  CORRECT: WHERE source_bank = 'kaspi'
                  WRONG:   WHERE source = 'kaspi'  ← crashes
  payer_bank    = bank name of the sender (free text, e.g. 'АО "KASPI BANK"')
  receiver_bank = bank name of the recipient (free text)
  When the user mentions a bank that is NOT kaspi/halyk (e.g. "RED BANK", "Halyk Bank"
  as a counterparty, "Jusan", "Freedom"), search payer_bank AND receiver_bank with LIKE:
  CORRECT: LOWER(COALESCE(payer_bank,'')) LIKE '%red bank%'
           OR LOWER(COALESCE(receiver_bank,'')) LIKE '%red bank%'
  Also check payer_name / receiver_name when the bank name may appear there.
- BRACKET RULE: When combining a date filter AND topic LIKE filters, ALWAYS wrap LIKE in parentheses:
  CORRECT: WHERE operation_date BETWEEN '...' AND '...' AND (LIKE ... OR LIKE ...)
  WRONG:   WHERE operation_date BETWEEN '...' AND '...' AND LIKE ... OR LIKE ...
  The second form breaks operator precedence — OR removes the date filter.
- CLIENT / STATEMENT FIELDS: The view already includes statements columns via LEFT JOIN.
  Use client_name, account_iban, period_from, period_to, opening_balance, closing_balance,
  total_debit, total_credit directly — NO need to write a JOIN yourself.
  CORRECT: WHERE client_name ILIKE '%ASYLBEK%'
  WRONG:   JOIN afm.statements ON ...  (view already has it)
- PERCENTILE for anomalies: use PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY amount_kzt)
  Example: WHERE amount_kzt > (SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY amount_kzt) FROM {NL_VIEW} WHERE amount_kzt IS NOT NULL)
- MEDIAN: use PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount_kzt)
- OBOROT (оборот) = SUM of all transactions regardless of direction. Use SUM(amount_kzt) without direction filter.

## TOPIC ANCHOR LOOKUP

| semantic_topic | Anchors to use in LIKE |
|----------------|------------------------|
| loan           | %займ%, %заем%, %кредит%, %долг%, %погаш% |
| deposit        | %депозит%, %вклад%, %процент%, %вознагражд% |
| tax            | %налог%, %ндс%, %кпн% |
| salary         | %зарплат%, %оклад%, %salary% |
| fee            | %комисси%, %fee%, %сбор% |
| utilities      | %коммун% |
| rent_lease     | %аренд%, %лизинг% |
| atm_cash       | %банкомат%, %налич%, %atm%, %cash%, %бан% |
| transfer       | %перевод%, %между счет% |
| penalty        | %штраф%, %пеня% |
| refund         | %возврат%, %возмещ% |
| goods_services | %товар%, %услуг% |
| purchase       | %покупк%, %магазин% |

## BUILT-IN EXAMPLES

### Example: DAILY AGG + LAG (SPIKE DETECTION)
Q: Найди дни, когда у клиента объем транзакций резко вырос
```sql
SELECT *
FROM (
    SELECT
        client_name,
        operation_date,
        daily_total,
        LAG(daily_total) OVER (
            PARTITION BY client_name
            ORDER BY operation_date
        ) AS prev_day_total
    FROM (
        SELECT
            client_name,
            operation_date,
            SUM(amount_kzt) AS daily_total
        FROM {NL_VIEW}
        WHERE amount_kzt IS NOT NULL
          AND client_name IS NOT NULL
          AND operation_date IS NOT NULL
        GROUP BY client_name, operation_date
    ) d
) t
WHERE prev_day_total IS NOT NULL
  AND daily_total > prev_day_total * 3
ORDER BY daily_total DESC
LIMIT 100;
```

### Example: RISK SCORE RANKING
Q: Ранжируй клиентов по risk score
```sql
SELECT
    client_name,
    SUM(CASE WHEN amount_kzt > 1000000 THEN 1 ELSE 0 END) AS large_tx,
    SUM(CASE WHEN payer_name = receiver_name THEN 1 ELSE 0 END) AS self_tx,
    COUNT(*) AS total_tx,
    (
        SUM(CASE WHEN amount_kzt > 1000000 THEN 1 ELSE 0 END) * 3 +
        SUM(CASE WHEN payer_name = receiver_name THEN 1 ELSE 0 END) * 5 +
        COUNT(*) * 0.1
    ) AS risk_score
FROM {NL_VIEW}
WHERE amount_kzt IS NOT NULL
  AND client_name IS NOT NULL
GROUP BY client_name
ORDER BY risk_score DESC
LIMIT 20;
```

### Example: SELF-TRANSFER DETECTION
Q: найди переводы самому себе
```sql
SELECT tx_id, operation_date, amount_kzt, payer_name, receiver_name, purpose_text
FROM {NL_VIEW}
WHERE payer_name = receiver_name
  AND amount_kzt IS NOT NULL
  AND operation_date IS NOT NULL
ORDER BY amount_kzt DESC
LIMIT 100;
```

### Example: TOPIC SEARCH (налоги)
Q: налоговые платежи kaspi
```sql
SELECT tx_id, operation_date, amount_kzt, direction, payer_name, receiver_name, purpose_text
FROM {NL_VIEW}
WHERE source_bank = 'kaspi'
  AND (
      LOWER(COALESCE(purpose_text, ''))  LIKE '%налог%'
      OR LOWER(COALESCE(raw_note, ''))  LIKE '%налог%'
      OR LOWER(COALESCE(purpose_text, ''))  LIKE '%кпн%'
      OR LOWER(COALESCE(raw_note, ''))  LIKE '%ндс%'
  )
  AND amount_kzt IS NOT NULL
  AND operation_date IS NOT NULL
ORDER BY operation_date DESC
LIMIT 100;
```

### Example: AGGREGATION WITH NULL FILTER
Q: топ 10 получателей по сумме за 2024
```sql
SELECT receiver_name,
       SUM(amount_kzt) AS total_amount,
       COUNT(*)        AS tx_count
FROM {NL_VIEW}
WHERE operation_date BETWEEN '2024-01-01' AND '2024-12-31'
  AND amount_kzt IS NOT NULL
  AND receiver_name IS NOT NULL
GROUP BY receiver_name
ORDER BY total_amount DESC
LIMIT 10;
```

## FINAL INSTRUCTION

Return ONLY SQL.
NO explanation.
NO markdown.\
"""


# ─────────────────────────────────────────────────────────────────────────────
# Builder
# ─────────────────────────────────────────────────────────────────────────────

def build_prompt(plan: QueryPlan) -> str:
    blocks: list[str] = []

    # ── 1. System role ────────────────────────────────────────────────────────
    blocks.append(_SYSTEM_ROLE)
    blocks.append("─" * 60)

    # ── 2. Schema ─────────────────────────────────────────────────────────────
    blocks.append(schema_prompt_block())
    blocks.append("─" * 60)

    # ── 3. Strategy ───────────────────────────────────────────────────────────
    blocks.append(_build_strategy(plan.embedder_enabled))
    blocks.append("─" * 60)

    # ── 4. Extracted entities ─────────────────────────────────────────────────
    entity_lines: list[str] = ["DETECTED ENTITIES:"]

    entity_text = plan.entities.as_text()
    entity_lines.append(entity_text if entity_text != "(none detected)" else "(none)")

    if plan.entities.top_n:
        entity_lines.append(f"→ use LIMIT {plan.entities.top_n}")

    if plan.entities.semantic_topic:
        topic = plan.entities.semantic_topic
        has_structured = bool(
            plan.entities.amount
            or plan.entities.date_range
            or plan.entities.direction
            or plan.entities.currency
            or plan.entities.source_bank
        )
        if has_structured:
            entity_lines.append(f"→ semantic_topic: {topic} | suggested: TYPE 3 (HYBRID)")
        else:
            entity_lines.append(f"→ semantic_topic: {topic} | suggested: TYPE 2 (TOPIC) or TYPE 4 (SEMANTIC)")

    blocks.append("\n".join(entity_lines))
    blocks.append("─" * 60)

    # ── 5a. K-means catalog context (from CatalogEntityResolver) ─────────────
    if getattr(plan, "catalog_context", ""):
        blocks.append(plan.catalog_context)
        blocks.append("─" * 60)

    # ── 5b. Retrieved context ─────────────────────────────────────────────────
    context_lines: list[str] = []

    if plan.context.sample_values:
        context_lines.append("REAL VALUES FROM YOUR DATA (use as anchor hints if helpful):")
        context_lines.append(plan.context.sample_values_text())

    if plan.context.similar_examples:
        if context_lines:
            context_lines.append("")
        context_lines.append("SIMILAR PAST QUERIES (copy pattern if it matches):")
        context_lines.append(plan.context.examples_text())

    if context_lines:
        blocks.append("\n".join(context_lines))
        blocks.append("─" * 60)

    # ── 6. User question ──────────────────────────────────────────────────────
    blocks.append(f"USER QUESTION:\n{plan.question}")
    blocks.append("\nReturn SQL only.")

    return "\n\n".join(blocks)