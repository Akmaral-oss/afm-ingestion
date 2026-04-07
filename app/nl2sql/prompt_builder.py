from __future__ import annotations

"""
Prompt builder — v4.0
Собирает 6-блочный промпт для LLM:
  1. System role
  2. Database schema (включая 19 категорий)
  3. SQL rules
  4. Extracted entities
  5. Retrieved context
  6. User question
"""

from .query_models import QueryPlan
from .schema_registry import NL_VIEW, schema_prompt_block


_SYSTEM_ROLE = """\
Ты — PostgreSQL SQL-генератор для базы данных финансовых транзакций АФМ (Агентство по финансовому мониторингу).

Твоя задача: преобразовать вопрос на русском языке в один корректный SELECT запрос к PostgreSQL.
Генерируй SQL строго по предоставленной схеме, извлечённым фильтрам и примерам из контекста.

КОНТЕКСТ СИСТЕМЫ:
- Данные: банковские выписки Kaspi и Halyk банков Казахстана
- Каждая транзакция имеет предустановленную категорию (transaction_category) — не нужно угадывать по тексту
- Сумма в тенге: amount_kzt (основное поле для денежных запросов)
- Для баланса (пришло/ушло): signed_amount_kzt (уже со знаком ±)
"""


_CATEGORY_RULES = """\
## ПРАВИЛА КАТЕГОРИЙ (КРИТИЧЕСКИ ВАЖНО)

transaction_category — это ПРЯМАЯ КОЛОНКА в каждой строке, назначается автоматически при загрузке.
НЕ нужно определять категорию через LIKE по purpose_text — используй точный фильтр по коду.

### 19 кодов категорий:

| Код                   | Название              | Когда использовать                        |
|-----------------------|-----------------------|-------------------------------------------|
| P2P_ПЕРЕВОД           | P2P перевод           | card to card, переводы между физлицами    |
| ПОКУПКА_В_МАГАЗИНЕ    | Покупка в магазине    | QR PAY, POS, оплата товаров, kaspi.kz     |
| ВНУТРЕННЯЯ_ОПЕРАЦИЯ   | Внутренние операции   | перевод между своими счетами, на депозит  |
| СНЯТИЕ_НАЛИЧНЫХ       | Снятие наличных       | банкомат, ATM, получение наличных         |
| ПОГАШЕНИЕ_КРЕДИТА     | Погашение кредита     | погашение займа, долга, ипотеки           |
| ГЕМБЛИНГ              | Гемблинг              | ставки, казино, букмекер, 1xbet           |
| ОБЯЗАТЕЛЬНЫЙ_ПЛАТЕЖ   | Обязательные платежи  | КПН, ИПН, ОСМС, налоги, штрафы           |
| ГОСВЫПЛАТА            | Госвыплата            | пенсия, пособие, ЕНПФ                     |
| ЗАРПЛАТА              | Зарплата              | зп карта, зарплата, salary                |
| ПОПОЛНЕНИЕ_СЧЕТА      | Пополнение счёта      | пополнение карты, cash in                 |
| РАСЧЕТ_ПО_ДОГОВОРУ    | Расчёты по договору   | оплата за услуги по договору              |
| ОПЛАТА_СЧЕТ_ФАКТУРЫ   | Оплата по счёт-фактуре| счёт-фактура, invoice                     |
| ПЛАТЕЖ_НА_КАРТУ       | Платёж на карту       | платёж на конкретную карту                |
| ВАЛЮТНАЯ_ОПЕРАЦИЯ     | Валютная операция     | конвертация, обмен валют, forex           |
| ВЫДАЧА_ЗАЙМА          | Выдача займа          | выдача займа, микрокредит, рассрочка      |
| АЛИМЕНТЫ              | Алименты              | алименты                                  |
| ЦЕННЫЕ_БУМАГИ         | Ценные бумаги         | акции, облигации, брокер, KASE            |
| ВОЗВРАТ_СРЕДСТВ       | Возврат средств       | возврат, refund, сторно                   |
| ПРОЧЕЕ                | Прочее                | не определено                             |

### SQL шаблоны по категориям:

**Фильтр по категории:**
```sql
WHERE transaction_category = 'ЗАРПЛАТА'
```

**Разбивка по всем категориям:**
```sql
SELECT transaction_category_ru,
       COUNT(*) AS tx_count,
       SUM(amount_kzt) AS total_kzt
FROM afm.transactions_nl_view
WHERE amount_kzt IS NOT NULL
GROUP BY transaction_category_ru
ORDER BY tx_count DESC;
```

**Баланс по категориям (signed_amount_kzt):**
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

**Только надёжные (rule-based) классификации:**
```sql
WHERE transaction_category = 'ГЕМБЛИНГ'
  AND category_source = 'rule'
```

**Транзакции требующие проверки:**
```sql
WHERE needs_review = TRUE
ORDER BY operation_date DESC
```

**Группировка по группам:**
```sql
SELECT category_group,
       SUM(amount_kzt) AS total
FROM afm.transactions_nl_view
WHERE amount_kzt IS NOT NULL
GROUP BY category_group
ORDER BY total DESC;
```

### ОБЯЗАТЕЛЬНЫЕ ПРАВИЛА:

- "по категории Зарплата" → WHERE transaction_category = 'ЗАРПЛАТА'  (НЕ LIKE!)
- "гемблинг операции" → WHERE transaction_category = 'ГЕМБЛИНГ'
- "внутренние переводы" → WHERE transaction_category = 'ВНУТРЕННЯЯ_ОПЕРАЦИЯ'
- "снятие наличных" → WHERE transaction_category = 'СНЯТИЕ_НАЛИЧНЫХ'
- Для баланса ВСЕГДА используй signed_amount_kzt, не CASE WHEN direction
- Для отображения используй transaction_category_ru (русское название)
"""


_SQL_RULES = f"""\
SQL ПРАВИЛА (соблюдать строго):

1.  Генерируй только один SELECT запрос.
2.  Используй только view: {NL_VIEW}
3.  Никогда не обращайся к raw таблицам (transactions_core, statements и т.д.).
4.  Всегда добавляй LIMIT (по умолчанию 100) кроме агрегационных запросов с GROUP BY.
5.  Для фильтрации по дате используй operation_date.
6.  Для денежных сравнений используй amount_kzt (если не указана другая валюта).
7.  Для агрегаций: GROUP BY + ORDER BY метрика DESC.
8.  Возвращай понятные колонки: даты, имена, суммы, назначение, направление.
9.  Не используй DROP, DELETE, UPDATE, INSERT, ALTER, CREATE.
10. Без объяснений — только SQL.

ФИЛЬТРЫ ПО КАТЕГОРИЯМ:
11. Когда пользователь называет категорию или тип операции → СРАЗУ используй WHERE transaction_category = '<КОД>'
12. Не используй LIKE по purpose_text для поиска по категориям — используй transaction_category.

ФИЛЬТРЫ ПО СТРУКТУРЕ:
13. Даты, суммы, направление (credit/debit), валюта, плательщик/получатель → WHERE.
14. 4-значное число в "за 2024", "в 2025" = ГОД, не сумма.
15. "большие", "крупные", "large" → amount_kzt > 1000000.

ТЕКСТОВЫЕ ФИЛЬТРЫ (только если категория не подходит):
16. Гибкие LIKE: LOWER(COALESCE(purpose_text, '')) LIKE '%ключевое_слово%'
17. Проверяй оба поля: purpose_text И semantic_text.

НАПРАВЛЕНИЕ:
18. "расходы", "траты", "списания" → direction = 'debit'
19. "поступления", "приход", "доходы" → direction = 'credit'

СЕМАНТИЧЕСКИЙ ПОИСК:
20. Для похожих/связанных запросов без точного текста → ORDER BY semantic_embedding <-> :query_embedding
21. Для последних транзакций → ORDER BY operation_date DESC
22. Для крупных → ORDER BY amount_kzt DESC

ВЫВОД:
23. Для поиска транзакций: tx_id, operation_date, amount_kzt, direction, transaction_category_ru, purpose_text, payer_name, receiver_name
24. Для агрегаций: только нужные колонки.
25. Никогда не делай SELECT *.
"""


def build_prompt(plan: QueryPlan) -> str:
    """
    Assembles the full prompt from 6 blocks.
    """
    blocks: list[str] = []

    # 1. System role
    blocks.append(f"[ROLE]\n{_SYSTEM_ROLE.strip()}")

    # 2. Schema
    blocks.append(f"[SCHEMA]\n{schema_prompt_block()}")

    # 3. Category rules
    blocks.append(f"[CATEGORY_RULES]\n{_CATEGORY_RULES.strip()}")

    # 4. SQL rules
    blocks.append(f"[SQL_RULES]\n{_SQL_RULES.strip()}")

    # 5. Extracted entities
    if plan.entities:
        ent_lines = []
        e = plan.entities
        if e.date_from:
            ent_lines.append(f"date_from: {e.date_from}")
        if e.date_to:
            ent_lines.append(f"date_to: {e.date_to}")
        if e.year:
            ent_lines.append(f"year: {e.year}")
        if e.amount_min is not None:
            ent_lines.append(f"amount_min: {e.amount_min}")
        if e.amount_max is not None:
            ent_lines.append(f"amount_max: {e.amount_max}")
        if e.direction:
            ent_lines.append(f"direction: {e.direction}")
        if e.currency:
            ent_lines.append(f"currency: {e.currency}")
        if e.source_bank:
            ent_lines.append(f"source_bank: {e.source_bank}")
        if e.top_n:
            ent_lines.append(f"top_n: {e.top_n}")
        if e.payer_name:
            ent_lines.append(f"payer_name: {e.payer_name}")
        if e.receiver_name:
            ent_lines.append(f"receiver_name: {e.receiver_name}")
        if e.purpose_keywords:
            ent_lines.append(f"purpose_keywords: {e.purpose_keywords}")
        if ent_lines:
            blocks.append("[EXTRACTED_FILTERS]\n" + "\n".join(ent_lines))

    # 6. Retrieved context (similar queries / examples)
    if plan.context_chunks:
        ctx = "\n---\n".join(plan.context_chunks[:5])
        blocks.append(f"[EXAMPLES]\n{ctx}")

    # 7. Question
    blocks.append(f"[QUESTION]\n{plan.question}")

    blocks.append("[SQL]")  # LLM starts writing SQL here

    return "\n\n".join(blocks)
