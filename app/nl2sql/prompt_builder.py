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
# Block 3 — Strategy (decision tree + examples)
# ─────────────────────────────────────────────────────────────────────────────

_STRATEGY = f"""\
## HARD RULES
- SELECT only. No DDL/DML. No raw tables — only {NL_VIEW}.
- LIMIT 100 by default. Omit LIMIT only for GROUP BY aggregations.
- Never SELECT semantic_embedding or SELECT *.
- Use operation_date for dates. Use amount_kzt for amounts (KZT default).
- A standalone 4-digit year (2024, 2025) is a DATE filter, never an amount.
- NEVER use LIKE on semantic_text. It contains operation_type_raw which includes
  accounting terms "кредит/дебет" — this causes false positives on ALL credit/debit
  transactions. semantic_text is ONLY for ORDER BY semantic_embedding <-> :query_embedding.
- For text LIKE searches use ONLY: purpose_text and raw_note.
- DEDUPLICATE LIKE conditions: if the same anchor (%x%) appears multiple times, use it once.
  Example: (LIKE '%налог%' OR LIKE '%кпн%') instead of (LIKE '%налог%' OR LIKE '%налог%' OR LIKE '%кпн%')
- DO NOT select duplicate columns in LIKE conditions in the same WHERE clause.

## STEP 1 — IDENTIFY QUERY TYPE

Look at the question and pick ONE type:

| Type | When to use |
|------|-------------|
| 1 FILTER     | Only structured constraints (date, amount, direction, currency, bank). No topic. |
| 2 TOPIC      | Named business category (кредит, налог, аренда, зарплата…). Possibly + structured filters. |
| 3 HYBRID     | Named topic + structured filters. Combine both in WHERE, use semantic ranking. |
| 4 SEMANTIC   | Vague / "похожие" / "связанные" / no obvious anchor. Use vector ranking only. |
| 5 AGGREGATION| "топ N", "суммарно", "сколько", "за период", GROUP BY needed. |
| 6 TRANSIT    | "транзитные счета", "промежуточные счета" — entities acting as pass-through. |
| 7 CIRCULAR   | "круговые транзакции", "самоперевод", money-loop A→B→A. |
| 8 AML_SCHEME | "обнал по типам", "схемы ИП", "подозрительные схемы", fraud pattern aggregation. |
| 9 CLIENT     | "поступления по клиенту", "операции клиента X" — filter by client_name / receiver_name. |

## STEP 2 — APPLY THE MATCHING PATTERN

### TYPE 1 — PURE FILTER
```sql
SELECT tx_id, operation_date, amount_kzt, direction,
       payer_name, receiver_name, purpose_text
FROM {NL_VIEW}
WHERE <structured_filters>
ORDER BY operation_date DESC
LIMIT 100;
```

### TYPE 2 — TOPIC SEARCH
Use LIKE anchors on purpose_text and raw_note ONLY.
NEVER apply LIKE to semantic_text — it contains accounting terms like "кредит/дебет"
that will cause false positives on every credit/debit transaction.
Put ALL LIKE conditions in ONE parenthesized OR block.
```sql
SELECT tx_id, operation_date, amount_kzt, direction,
       payer_name, receiver_name, purpose_text
FROM {NL_VIEW}
WHERE (
    LOWER(COALESCE(purpose_text, '')) LIKE '%<anchor1>%'
    OR LOWER(COALESCE(raw_note, ''))  LIKE '%<anchor1>%'
    OR LOWER(COALESCE(purpose_text, '')) LIKE '%<anchor2>%'
    OR LOWER(COALESCE(raw_note, ''))  LIKE '%<anchor2>%'
)
  AND <structured_filters_if_any>
ORDER BY operation_date DESC
LIMIT 100;
```

### TYPE 3 — HYBRID (topic + structured)
```sql
SELECT tx_id, operation_date, amount_kzt, direction,
       payer_name, receiver_name, purpose_text
FROM {NL_VIEW}
WHERE <structured_filters>
  AND (
    LOWER(COALESCE(purpose_text, '')) LIKE '%<anchor>%'
    OR LOWER(COALESCE(raw_note, ''))  LIKE '%<anchor>%'
  )
ORDER BY semantic_embedding <-> :query_embedding
LIMIT 100;
```
IMPORTANT: :query_embedding is the exact placeholder name. Always use it as-is.

### TYPE 4 — SEMANTIC / SIMILARITY
```sql
SELECT tx_id, operation_date, amount_kzt, direction,
       payer_name, receiver_name, purpose_text
FROM {NL_VIEW}
WHERE <structured_filters_if_any>
ORDER BY semantic_embedding <-> :query_embedding
LIMIT 100;
```

### TYPE 5 — AGGREGATION
```sql
SELECT receiver_name,
       SUM(amount_kzt) AS total_amount,
       COUNT(*)        AS tx_count
FROM {NL_VIEW}
WHERE receiver_name IS NOT NULL
  AND <filters_if_any>
GROUP BY receiver_name
ORDER BY total_amount DESC
LIMIT 20;
```

IMPORTANT AGGREGATION RULES:
- Always filter out NULL receiver_name: WHERE receiver_name IS NOT NULL
- For payer aggregations: WHERE payer_name IS NOT NULL
- For date aggregations: WHERE operation_date IS NOT NULL
- This prevents NULL groups from polluting the results

### TYPE 6 — TRANSIT ACCOUNTS (транзитные счета)
Entities appearing as BOTH payer AND receiver with high pass-through ratio (in ≈ out).
ALWAYS use this exact CTE structure — do NOT use payer_name = receiver_name.
```sql
WITH inflow AS (
    SELECT receiver_name AS entity, COUNT(*) AS cnt_in, SUM(amount_kzt) AS total_in
    FROM {NL_VIEW} WHERE receiver_name IS NOT NULL GROUP BY receiver_name
),
outflow AS (
    SELECT payer_name AS entity, COUNT(*) AS cnt_out, SUM(amount_kzt) AS total_out
    FROM {NL_VIEW} WHERE payer_name IS NOT NULL GROUP BY payer_name
)
SELECT i.entity, i.cnt_in, o.cnt_out,
       ROUND(i.total_in::numeric,2)  AS total_in,
       ROUND(o.total_out::numeric,2) AS total_out,
       ROUND((LEAST(i.total_in, o.total_out)
              / NULLIF(GREATEST(i.total_in, o.total_out),0)*100)::numeric,1) AS passthrough_pct
FROM inflow i JOIN outflow o ON i.entity = o.entity
WHERE i.cnt_in >= 3 AND o.cnt_out >= 3
  AND LEAST(i.total_in, o.total_out)
      / NULLIF(GREATEST(i.total_in, o.total_out),0) > 0.7
ORDER BY passthrough_pct DESC, i.total_in DESC
LIMIT 50;
```

### TYPE 7 — CIRCULAR TRANSACTIONS (круговые транзакции)
For self-transfers (payer = receiver):
```sql
SELECT tx_id, operation_date, amount_kzt, payer_name, receiver_name, purpose_text
FROM {NL_VIEW}
WHERE payer_name = receiver_name AND payer_name IS NOT NULL
ORDER BY operation_date DESC LIMIT 100;
```
For TRUE circular flow A→B→A (2-hop JOIN):
```sql
SELECT t1.tx_id AS tx1_id, t2.tx_id AS tx2_id,
       t1.payer_name AS entity_a, t1.receiver_name AS entity_b,
       t1.amount_kzt AS a_to_b, t2.amount_kzt AS b_to_a,
       t1.operation_date AS date1, t2.operation_date AS date2
FROM {NL_VIEW} t1
JOIN {NL_VIEW} t2
  ON t1.receiver_name = t2.payer_name
 AND t2.receiver_name = t1.payer_name
 AND t1.tx_id <> t2.tx_id
 AND t1.payer_name <> t1.receiver_name
 AND t2.operation_date BETWEEN t1.operation_date AND t1.operation_date + INTERVAL '30 days'
WHERE t1.payer_name IS NOT NULL
ORDER BY t1.operation_date DESC LIMIT 100;
```

### TYPE 8 — AML SCHEME DETECTION

#### Obnal by operation type (обнал по типам):
```sql
SELECT operation_type_raw,
       COUNT(*) AS tx_count, SUM(amount_kzt) AS total_amount,
       ROUND(AVG(amount_kzt)::numeric,2) AS avg_amount,
       COUNT(DISTINCT receiver_name) AS unique_receivers
FROM {NL_VIEW}
WHERE direction = 'debit' AND operation_type_raw IS NOT NULL
GROUP BY operation_type_raw
ORDER BY total_amount DESC LIMIT 30;
```

#### Total obnal for a year (общий обнал за 2024):
Replace year in date filter from extracted entities.
```sql
SELECT EXTRACT(YEAR FROM operation_date)::int AS year,
       operation_type_raw,
       COUNT(*) AS tx_count,
       SUM(amount_kzt) AS total_obnal,
       COUNT(DISTINCT receiver_name) AS unique_receivers
FROM {NL_VIEW}
WHERE direction = 'debit'
  AND operation_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY year, operation_type_raw
ORDER BY total_obnal DESC;
```

#### IP entrepreneur suspicious schemes (подозрительные схемы ИП):
```sql
SELECT receiver_name,
       COUNT(*) AS tx_count, SUM(amount_kzt) AS total_amount,
       ROUND(AVG(amount_kzt)::numeric,2) AS avg_amount,
       COUNT(DISTINCT payer_name) AS unique_payers,
       MIN(operation_date) AS first_tx, MAX(operation_date) AS last_tx
FROM {NL_VIEW}
WHERE (LOWER(COALESCE(receiver_name,'')) LIKE '%ип %'
    OR LOWER(COALESCE(receiver_name,'')) LIKE '% ип%'
    OR LOWER(COALESCE(receiver_name,'')) LIKE '%индивидуальный%')
  AND receiver_name IS NOT NULL
GROUP BY receiver_name
HAVING COUNT(*) >= 5 AND SUM(amount_kzt) > 1000000
ORDER BY total_amount DESC LIMIT 30;
```

#### Top banks by withdrawal (топ банков по снятию):
```sql
SELECT source_bank, COUNT(*) AS tx_count,
       SUM(amount_kzt) AS total_withdrawal,
       ROUND(AVG(amount_kzt)::numeric,2) AS avg_withdrawal
FROM {NL_VIEW}
WHERE direction = 'debit' AND source_bank IS NOT NULL
GROUP BY source_bank ORDER BY total_withdrawal DESC LIMIT 20;
```

### TYPE 9 — CLIENT TRANSACTIONS (поступления/операции по клиенту)
Use client_name or receiver_name for ILIKE filter. Default direction='credit' for incoming.
If no specific client name is mentioned, filter by direction only.
```sql
SELECT tx_id, operation_date, amount_kzt,
       payer_name, client_name, receiver_name, purpose_text, source_bank
FROM {NL_VIEW}
WHERE direction = 'credit'
  AND (LOWER(COALESCE(client_name,''))   ILIKE '%<client>%'
    OR LOWER(COALESCE(receiver_name,'')) ILIKE '%<client>%')
ORDER BY operation_date DESC LIMIT 100;
```
For period aggregation (сумма за период):
```sql
SELECT TO_CHAR(operation_date,'YYYY-MM') AS period,
       SUM(CASE WHEN direction='credit' THEN amount_kzt ELSE 0 END) AS total_credit,
       SUM(CASE WHEN direction='debit'  THEN amount_kzt ELSE 0 END) AS total_debit,
       COUNT(*) AS tx_count
FROM {NL_VIEW}
WHERE operation_date BETWEEN '<from>' AND '<to>'
GROUP BY period ORDER BY period;
```

## STEP 3 — TOPIC ANCHOR LOOKUP

### LIKE-based topics (use in WHERE purpose_text/raw_note):

| semantic_topic    | Anchors to use in LIKE |
|-------------------|------------------------|
| loan              | %займ%, %заем%, %кредит%, %долг%, %погаш% |
| deposit           | %депозит%, %вклад%, %процент%, %вознагражд% |
| tax               | %налог%, %ндс%, %кпн% |
| salary            | %зарплат%, %оклад%, %salary% |
| fee               | %комисси%, %fee%, %сбор% |
| utilities         | %коммун% |
| rent_lease        | %аренд%, %лизинг% |
| atm_cash          | %банкомат%, %налич%, %atm%, %cash% |
| transfer          | %перевод%, %между счет% |
| penalty           | %штраф%, %пеня% |
| refund            | %возврат%, %возмещ% |
| goods_services    | %товар%, %услуг% |
| purchase          | %покупк%, %магазин% |
| real_estate       | %недвижим%, %квартир%, %участок% (TYPE 2 TOPIC) |

### Structural/aggregation topics (use TYPE 5–9 patterns, NOT LIKE):

| semantic_topic    | Pattern to use |
|-------------------|----------------|
| transit           | TYPE 6 — CTE inflow/outflow passthrough |
| circular          | TYPE 7 — payer=receiver or 2-hop JOIN |
| cash_out          | TYPE 5 — direction='debit' GROUP BY operation_type_raw |
| cash_out_by_type  | TYPE 8 — obnal by type |
| ip_entrepreneur   | TYPE 8 — GROUP BY receiver_name HAVING ИП filter |
| top_banks         | TYPE 5 — direction='debit' GROUP BY source_bank |
| client_incoming   | TYPE 9 — direction='credit' + client ILIKE filter |
| period_summary    | TYPE 9 — GROUP BY TO_CHAR(operation_date,'YYYY-MM') |
| suspicious        | TYPE 8 — combine multiple AML conditions |

## ADVANCED PATTERNS FOR FINANCIAL CRIMES

### CIRCULAR TRANSACTIONS (переводы самому себе / круговые)
Self-transfer — simplest form:
```sql
SELECT tx_id, operation_date, amount_kzt, payer_name, receiver_name, purpose_text
FROM afm.transactions_nl_view
WHERE payer_name = receiver_name AND payer_name IS NOT NULL
ORDER BY operation_date DESC LIMIT 100;
```
True 2-hop circular A→B→A:
```sql
SELECT t1.tx_id AS tx1, t2.tx_id AS tx2,
       t1.payer_name AS entity_a, t1.receiver_name AS entity_b,
       t1.amount_kzt AS a_to_b, t2.amount_kzt AS b_to_a,
       t1.operation_date, t2.operation_date AS return_date
FROM afm.transactions_nl_view t1
JOIN afm.transactions_nl_view t2
  ON t1.receiver_name = t2.payer_name
 AND t2.receiver_name = t1.payer_name
 AND t1.tx_id <> t2.tx_id
 AND t1.payer_name <> t1.receiver_name
 AND t2.operation_date BETWEEN t1.operation_date AND t1.operation_date + INTERVAL '30 days'
WHERE t1.payer_name IS NOT NULL
ORDER BY t1.operation_date DESC LIMIT 100;
```

### TRANSIT ACCOUNTS (транзитные счета)
NEVER use payer_name = receiver_name for transit. Use the CTE throughput pattern:
```sql
WITH inflow AS (
    SELECT receiver_name AS entity, COUNT(*) AS cnt_in, SUM(amount_kzt) AS total_in
    FROM afm.transactions_nl_view WHERE receiver_name IS NOT NULL GROUP BY receiver_name
),
outflow AS (
    SELECT payer_name AS entity, COUNT(*) AS cnt_out, SUM(amount_kzt) AS total_out
    FROM afm.transactions_nl_view WHERE payer_name IS NOT NULL GROUP BY payer_name
)
SELECT i.entity, i.cnt_in, o.cnt_out,
       ROUND(i.total_in::numeric,2) AS total_in,
       ROUND(o.total_out::numeric,2) AS total_out,
       ROUND((LEAST(i.total_in,o.total_out)/NULLIF(GREATEST(i.total_in,o.total_out),0)*100)::numeric,1)
         AS passthrough_pct
FROM inflow i JOIN outflow o ON i.entity = o.entity
WHERE i.cnt_in >= 3 AND o.cnt_out >= 3
  AND LEAST(i.total_in,o.total_out)/NULLIF(GREATEST(i.total_in,o.total_out),0) > 0.7
ORDER BY passthrough_pct DESC, i.total_in DESC LIMIT 50;
```

### OBNAL BY OPERATION TYPE (обнал по типам)
```sql
SELECT operation_type_raw,
       COUNT(*) AS tx_count, SUM(amount_kzt) AS total_amount,
       ROUND(AVG(amount_kzt)::numeric,2) AS avg_amount,
       COUNT(DISTINCT receiver_name) AS unique_receivers
FROM afm.transactions_nl_view
WHERE direction = 'debit' AND operation_type_raw IS NOT NULL
GROUP BY operation_type_raw ORDER BY total_amount DESC LIMIT 30;
```

### TOTAL OBNAL FOR YEAR (общий обнал за год)
```sql
SELECT EXTRACT(YEAR FROM operation_date)::int AS year,
       operation_type_raw,
       COUNT(*) AS tx_count, SUM(amount_kzt) AS total_obnal,
       COUNT(DISTINCT receiver_name) AS unique_receivers
FROM afm.transactions_nl_view
WHERE direction = 'debit'
  AND operation_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY year, operation_type_raw ORDER BY total_obnal DESC;
```

### REAL ESTATE TRANSACTIONS (недвижимость)
```sql
SELECT tx_id, operation_date, amount_kzt, payer_name, receiver_name, purpose_text
FROM afm.transactions_nl_view
WHERE (LOWER(COALESCE(purpose_text,'')) LIKE '%недвижим%'
    OR LOWER(COALESCE(purpose_text,'')) LIKE '%квартир%'
    OR LOWER(COALESCE(purpose_text,'')) LIKE '%участок%'
    OR LOWER(COALESCE(raw_note,''))     LIKE '%недвижим%')
ORDER BY amount_kzt DESC LIMIT 100;
```

### IP ENTREPRENEUR SUSPICIOUS SCHEMES (подозрительные схемы ИП)
High aggregate amounts received from many different payers — shell-IP indicator:
```sql
SELECT receiver_name,
       COUNT(*) AS tx_count, SUM(amount_kzt) AS total_amount,
       ROUND(AVG(amount_kzt)::numeric,2) AS avg_amount,
       COUNT(DISTINCT payer_name) AS unique_payers,
       MIN(operation_date) AS first_tx, MAX(operation_date) AS last_tx
FROM afm.transactions_nl_view
WHERE (LOWER(COALESCE(receiver_name,'')) LIKE '%ип %'
    OR LOWER(COALESCE(receiver_name,'')) LIKE '% ип%'
    OR LOWER(COALESCE(receiver_name,'')) LIKE '%индивидуальный%')
  AND receiver_name IS NOT NULL
GROUP BY receiver_name
HAVING COUNT(*) >= 5 AND SUM(amount_kzt) > 1000000
ORDER BY total_amount DESC LIMIT 30;
```

### TOP BANKS BY WITHDRAWAL (топ банков по снятию)
```sql
SELECT source_bank, COUNT(*) AS tx_count,
       SUM(amount_kzt) AS total_withdrawal,
       ROUND(AVG(amount_kzt)::numeric,2) AS avg_withdrawal
FROM afm.transactions_nl_view
WHERE direction = 'debit' AND source_bank IS NOT NULL
GROUP BY source_bank ORDER BY total_withdrawal DESC LIMIT 20;
```

### CLIENT INCOMING (поступления по клиенту)
```sql
SELECT tx_id, operation_date, amount_kzt,
       payer_name, client_name, receiver_name, purpose_text, source_bank
FROM afm.transactions_nl_view
WHERE direction = 'credit'
  AND (LOWER(COALESCE(client_name,''))   ILIKE '%иванов%'
    OR LOWER(COALESCE(receiver_name,'')) ILIKE '%иванов%')
ORDER BY operation_date DESC LIMIT 100;
```

### PERIOD AGGREGATION (сумма операций за период)
```sql
SELECT TO_CHAR(operation_date,'YYYY-MM') AS period,
       SUM(CASE WHEN direction='credit' THEN amount_kzt ELSE 0 END) AS total_credit,
       SUM(CASE WHEN direction='debit'  THEN amount_kzt ELSE 0 END) AS total_debit,
       COUNT(*) AS tx_count
FROM afm.transactions_nl_view
WHERE operation_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY period ORDER BY period;
```

## BUILT-IN EXAMPLES

### Example A — TYPE 3 (loan + amount + year)
Q: платежи по займам больше 5 млн за 2024
```sql
SELECT tx_id, operation_date, amount_kzt, direction, payer_name, receiver_name, purpose_text
FROM afm.transactions_nl_view
WHERE amount_kzt > 5000000
  AND operation_date BETWEEN '2024-01-01' AND '2024-12-31'
  AND (
      LOWER(COALESCE(purpose_text, ''))  LIKE '%займ%'
      OR LOWER(COALESCE(raw_note, ''))  LIKE '%займ%'
      OR LOWER(COALESCE(purpose_text, ''))  LIKE '%кредит%'
      OR LOWER(COALESCE(raw_note, ''))  LIKE '%кредит%'
  )
ORDER BY semantic_embedding <-> :query_embedding
LIMIT 100;
```

### Example B — TYPE 5 (top receivers by year)
Q: топ 10 получателей по сумме за 2024
```sql
SELECT receiver_name,
       SUM(amount_kzt) AS total_amount,
       COUNT(*)        AS tx_count
FROM afm.transactions_nl_view
WHERE operation_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY receiver_name
ORDER BY total_amount DESC
LIMIT 10;
```

### Example C — TYPE 2 (topic + bank filter)
Q: налоговые платежи kaspi
```sql
SELECT tx_id, operation_date, amount_kzt, direction, payer_name, receiver_name, purpose_text
FROM afm.transactions_nl_view
WHERE source_bank = 'kaspi'
  AND (
      LOWER(COALESCE(purpose_text, ''))  LIKE '%налог%'
      OR LOWER(COALESCE(raw_note, ''))  LIKE '%налог%'
      OR LOWER(COALESCE(purpose_text, ''))  LIKE '%кпн%'
      OR LOWER(COALESCE(raw_note, ''))  LIKE '%ндс%'
  )
ORDER BY operation_date DESC
LIMIT 100;
```

### Example D — TYPE 1 (structured filter only)
Q: последние 50 входящих переводов
```sql
SELECT tx_id, operation_date, amount_kzt, payer_name, receiver_name, purpose_text
FROM afm.transactions_nl_view
WHERE direction = 'credit'
ORDER BY operation_date DESC
LIMIT 50;
```

### Example E — TYPE 4 (pure semantic)
Q: транзакции похожие на выплату аренды
```sql
SELECT tx_id, operation_date, amount_kzt, direction, payer_name, receiver_name, purpose_text
FROM afm.transactions_nl_view
ORDER BY semantic_embedding <-> :query_embedding
LIMIT 100;
```

### Example F — TYPE 6 (transit accounts)
Q: Найти транзитные счета
```sql
WITH inflow AS (
    SELECT receiver_name AS entity, COUNT(*) AS cnt_in, SUM(amount_kzt) AS total_in
    FROM afm.transactions_nl_view WHERE receiver_name IS NOT NULL GROUP BY receiver_name
),
outflow AS (
    SELECT payer_name AS entity, COUNT(*) AS cnt_out, SUM(amount_kzt) AS total_out
    FROM afm.transactions_nl_view WHERE payer_name IS NOT NULL GROUP BY payer_name
)
SELECT i.entity, i.cnt_in, o.cnt_out,
       ROUND(i.total_in::numeric,2) AS total_in,
       ROUND(o.total_out::numeric,2) AS total_out,
       ROUND((LEAST(i.total_in,o.total_out)/NULLIF(GREATEST(i.total_in,o.total_out),0)*100)::numeric,1) AS passthrough_pct
FROM inflow i JOIN outflow o ON i.entity = o.entity
WHERE i.cnt_in >= 3 AND o.cnt_out >= 3
  AND LEAST(i.total_in,o.total_out)/NULLIF(GREATEST(i.total_in,o.total_out),0) > 0.7
ORDER BY passthrough_pct DESC, i.total_in DESC LIMIT 50;
```

### Example G — TYPE 8 (obnal by type)
Q: Обнал по типам
```sql
SELECT operation_type_raw,
       COUNT(*) AS tx_count, SUM(amount_kzt) AS total_amount,
       ROUND(AVG(amount_kzt)::numeric,2) AS avg_amount,
       COUNT(DISTINCT receiver_name) AS unique_receivers
FROM afm.transactions_nl_view
WHERE direction = 'debit' AND operation_type_raw IS NOT NULL
GROUP BY operation_type_raw ORDER BY total_amount DESC LIMIT 30;
```

### Example H — TYPE 8 (total obnal by year)
Q: Общий обнал за 2024
```sql
SELECT EXTRACT(YEAR FROM operation_date)::int AS year,
       operation_type_raw,
       COUNT(*) AS tx_count, SUM(amount_kzt) AS total_obnal,
       COUNT(DISTINCT receiver_name) AS unique_receivers
FROM afm.transactions_nl_view
WHERE direction = 'debit'
  AND operation_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY year, operation_type_raw ORDER BY total_obnal DESC;
```

### Example I — TYPE 8 (IP suspicious schemes)
Q: Подозрительные схемы ИП
```sql
SELECT receiver_name,
       COUNT(*) AS tx_count, SUM(amount_kzt) AS total_amount,
       ROUND(AVG(amount_kzt)::numeric,2) AS avg_amount,
       COUNT(DISTINCT payer_name) AS unique_payers,
       MIN(operation_date) AS first_tx, MAX(operation_date) AS last_tx
FROM afm.transactions_nl_view
WHERE (LOWER(COALESCE(receiver_name,'')) LIKE '%ип %'
    OR LOWER(COALESCE(receiver_name,'')) LIKE '% ип%'
    OR LOWER(COALESCE(receiver_name,'')) LIKE '%индивидуальный%')
  AND receiver_name IS NOT NULL
GROUP BY receiver_name
HAVING COUNT(*) >= 5 AND SUM(amount_kzt) > 1000000
ORDER BY total_amount DESC LIMIT 30;
```

### Example J — TYPE 5 (top banks by withdrawal)
Q: Топ банков по снятию средств
```sql
SELECT source_bank, COUNT(*) AS tx_count,
       SUM(amount_kzt) AS total_withdrawal,
       ROUND(AVG(amount_kzt)::numeric,2) AS avg_withdrawal
FROM afm.transactions_nl_view
WHERE direction = 'debit' AND source_bank IS NOT NULL
GROUP BY source_bank ORDER BY total_withdrawal DESC LIMIT 20;
```

### Example K — TYPE 9 (client incoming)
Q: Показать все поступления по клиенту Иванов
```sql
SELECT tx_id, operation_date, amount_kzt,
       payer_name, client_name, receiver_name, purpose_text, source_bank
FROM afm.transactions_nl_view
WHERE direction = 'credit'
  AND (LOWER(COALESCE(client_name,''))   ILIKE '%иванов%'
    OR LOWER(COALESCE(receiver_name,'')) ILIKE '%иванов%')
ORDER BY operation_date DESC LIMIT 100;
```

### Example L — TYPE 9 (period aggregation)
Q: Сумма операций за 2024
```sql
SELECT TO_CHAR(operation_date,'YYYY-MM') AS period,
       SUM(CASE WHEN direction='credit' THEN amount_kzt ELSE 0 END) AS total_credit,
       SUM(CASE WHEN direction='debit'  THEN amount_kzt ELSE 0 END) AS total_debit,
       COUNT(*) AS tx_count
FROM afm.transactions_nl_view
WHERE operation_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY period ORDER BY period;
```\
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
    blocks.append(_STRATEGY)
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

    # ── 5. Retrieved context ──────────────────────────────────────────────────
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