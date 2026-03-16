from __future__ import annotations

"""
Prompt builder.

Assembles the 6-block prompt sent to the LLM SQL generator:

  1. System role
  2. Database schema
  3. SQL rules
  4. Extracted entities
  5. Retrieved context
  6. User question
"""

from .query_models import QueryPlan
from .schema_registry import NL_VIEW, schema_prompt_block


_SYSTEM_ROLE = """\
You are a PostgreSQL SQL generator for a financial transactions database used by financial intelligence analysts.

Your task is to convert a natural language question into a single, safe PostgreSQL SELECT query.
You must produce accurate SQL grounded in the provided schema, extracted filters, and retrieved examples.
"""


_SQL_RULES = f"""\
SQL RULES (strictly follow all of them):
1.  Generate only a single SELECT statement.
2.  Use only the view: {NL_VIEW}
3.  Never reference raw tables (transactions_core, statements, etc.).
4.  Always include LIMIT (default 100) unless the query is an aggregation with GROUP BY.
5.  Use `operation_date` for date / period filtering.
6.  Use `amount_kzt` for monetary comparisons unless the question explicitly asks for a non-KZT currency amount.
7.  For aggregation queries use GROUP BY + ORDER BY metric DESC.
8.  Always return human-readable columns (dates, names, amounts, purpose_text, direction when relevant).
9.  Do not use DROP, DELETE, UPDATE, INSERT, ALTER, CREATE.
10. Do not add explanations — output raw SQL only.

STRUCTURED FILTER RULES:
11. If the question contains structured constraints such as date, amount, direction, currency, payer, receiver, bank, or source_bank,
    you MUST put them in the WHERE clause.
12. A 4-digit number in phrases like "за 2024", "в 2024", "за 2025 год" is a YEAR filter, not an amount.
13. Do not generate amount filters from standalone years.
14. Words like "большие", "крупные", "large", "high-value" imply an amount filter.
    If no exact threshold is given, use a reasonable threshold such as amount_kzt > 1000000.

TOPIC ANCHORS:
15. For loans / credit / debt topics use anchors such as:
       '%займ%', '%заем%', '%кредит%', '%долг%', '%погаш%'
16. For deposit / вклад / interest topics use anchors such as:
       '%депозит%', '%вклад%', '%процент%', '%вознагражд%'
17. For коммунальные услуги topics use anchors such as:
       '%коммун%'
18. For rent / lease topics use anchors such as:
       '%аренд%', '%лизинг%'
19. For goods / services topics use anchors such as:
       '%товар%', '%услуг%'
20. For cash withdrawal / ATM topics use anchors such as:
       '%банкомат%', '%налич%', '%atm%', '%cash%'
21. For internal transfer topics use anchors such as:
       '%перевод%', '%счет%', '%со счета%', '%на счет%', '%между счетами%'

TEXT FILTER STYLE:
22. Prefer flexible topic filters such as:
       LOWER(COALESCE(purpose_text, '')) LIKE '%коммун%'
    instead of overly exact full-string matches like:
       purpose_text LIKE '%коммунальные услуги%'
23. When topic filters are used, check BOTH:
       LOWER(COALESCE(purpose_text, ''))
       LOWER(COALESCE(semantic_text, ''))
24. Prefer grouped OR conditions for topic filters, for example:
       (
           LOWER(COALESCE(purpose_text, '')) LIKE '%депозит%'
           OR LOWER(COALESCE(semantic_text, '')) LIKE '%депозит%'
           OR LOWER(COALESCE(purpose_text, '')) LIKE '%вклад%'
           OR LOWER(COALESCE(semantic_text, '')) LIKE '%вклад%'
       )
25. When multiple topic LIKE conditions are used with OR, ALWAYS wrap them inside one parenthesized WHERE block.
26. Do not mix OR topic conditions loosely with other clauses. Keep topic filters grouped.

SEARCH MODE DISTINCTION:
27. If the question explicitly asks for rows where text contains a word, phrase, or exact keyword
    (examples: "contains", "содержит", "где в назначении есть", "where in purpose", explicit quoted words),
    use lexical topic filters in WHERE.
28. If the question names a clear business topic in an anchored way
    (examples: "по аренде", "по кредитам", "по депозитам", "коммунальные платежи"),
    use lexical topic filters in WHERE and optionally rank by semantic_embedding.
29. If the question asks for related / similar / associated / похожие / связанные transactions
    and does NOT require exact text match, allow broader semantic retrieval with FEWER lexical restrictions.
30. For semantic exploration queries, do not over-constrain the WHERE clause with narrow lexical filters derived directly from the user's paraphrase.
    If the paraphrase is abstract (for example: "снятие наличных", "оплата помещения", "обслуживание кредита"),
    prefer semantic ranking and only use broad supporting anchors if necessary.
31. If the question is purely semantic and has no structured filters, semantic ordering alone is allowed:
       ORDER BY semantic_embedding <-> :query_embedding
32. If the question is semantic exploration with structured filters, keep the structured filters in WHERE and prefer semantic ranking.
33. If both topic + structured constraints are present in an anchored query, generate HYBRID SQL:
       WHERE structured_filters AND topic_filters
       ORDER BY semantic_embedding <-> :query_embedding
34. If the question asks for both semantic topic and exact constraints, do both:
       structured WHERE + topic WHERE + semantic ranking.

ORDERING RULES:
35. For non-aggregation transaction search queries, include a meaningful ORDER BY:
       - ORDER BY semantic_embedding <-> :query_embedding for hybrid/semantic search
       - ORDER BY operation_date DESC for exact topical filtering without semantic ranking
       - ORDER BY amount_kzt DESC for largest / biggest / крупные queries
36. If topic + structured constraints are present, prefer ORDER BY semantic_embedding <-> :query_embedding.
37. If the question is about the latest / recent transactions, prefer ORDER BY operation_date DESC.

OUTPUT STYLE:
38. For topic-based transaction search queries, prefer returning:
       tx_id, operation_date, amount_kzt, direction, purpose_text, payer_name, receiver_name
39. For aggregation queries, return only the columns needed for the summary.
40. Do not SELECT semantic_embedding unless explicitly needed.
41. Never use SELECT *.

SEMANTIC SAFETY RULES:
42. Vector similarity is for ranking relevant rows, not for replacing explicit structured filters.
43. Do NOT rely only on ORDER BY semantic_embedding <-> :query_embedding when the user clearly requests exact keyword/topic matching.
44. Do NOT overuse lexical filters for semantic exploration queries like:
       "похожие", "similar", "related", "associated", "связанные"
    unless the user also gave exact structured constraints that require anchored filtering.
"""


def build_prompt(plan: QueryPlan) -> str:
    blocks: list[str] = []

    # 1. System role
    blocks.append(_SYSTEM_ROLE.strip())
    blocks.append("─" * 60)

    # 2. Schema
    blocks.append(schema_prompt_block())
    blocks.append("─" * 60)

    # 3. SQL rules
    blocks.append(_SQL_RULES.strip())
    blocks.append("─" * 60)

    # 4. Extracted entities
    blocks.append("DETECTED FILTERS (from the question):")
    blocks.append(plan.entities.as_text())

    if plan.entities.top_n:
        blocks.append(f"→ this is a top-{plan.entities.top_n} ranking query")

    if plan.entities.semantic_topic:
        blocks.append(
            f"→ semantic topic detected: {plan.entities.semantic_topic}. "
            "Classify the query as one of: "
            "(1) EXACT/ANCHORED topic retrieval, "
            "(2) HYBRID structured + topic retrieval, "
            "(3) BROAD SEMANTIC exploration. "
            "Use EXACT/ANCHORED when the user asks for explicit category/keyword/topic matching. "
            "Use BROAD SEMANTIC exploration when the user asks for similar/related/pохожие/связанные transactions "
            "without exact text-match intent. "
            "For BROAD SEMANTIC exploration, avoid narrow lexical filters derived from the paraphrase itself. "
            "If structured filters are present, preserve them in WHERE and prefer ORDER BY semantic_embedding <-> :query_embedding."
        )

    blocks.append("─" * 60)

    # 5. Retrieved context
    if plan.context.sample_values:
        blocks.append("SAMPLE VALUES FROM THE DATABASE (relevant to this question):")
        blocks.append(plan.context.sample_values_text())

    if plan.context.similar_examples:
        blocks.append("")
        blocks.append("SIMILAR SOLVED QUERIES (learn the SQL pattern):")
        blocks.append(plan.context.examples_text())

    blocks.append("─" * 60)

    # 6. User question
    blocks.append(f"USER QUESTION:\n{plan.question}")
    blocks.append("")
    blocks.append("Return the SQL query only — no markdown, no explanation.")

    return "\n\n".join(blocks)
