from __future__ import annotations

from .query_models import QueryPlan
from .schema_registry import NL_VIEW, schema_prompt_block

_SYSTEM_ROLE = """\
You are a PostgreSQL SQL generator for a financial transactions database used by financial intelligence analysts.

Your task is to convert a natural language question into a single, safe PostgreSQL SELECT query.
You must produce accurate SQL grounded in the provided schema, extracted filters, and retrieved examples.
"""


def _sql_rules(include_semantic: bool) -> str:
    rules = f"""\
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
11. Exclude obviously null / empty rows when the user asks for top transactions or meaningful lists.
12. If the question contains structured constraints such as date, amount, direction, currency, payer, receiver, bank, or source_bank,
    you MUST put them in the WHERE clause.
13. A 4-digit number in phrases like "за 2024", "в 2024", "за 2025 год" is a YEAR filter, not an amount.
14. Do not generate amount filters from standalone years.
15. Words like "большие", "крупные", "large", "high-value" imply an amount filter.
    If no exact threshold is given, use a reasonable threshold such as amount_kzt > 1000000.
16. Prefer flexible topic filters such as LOWER(COALESCE(purpose_text, '')) LIKE '%коммун%'.
17. When topic filters are used, check BOTH purpose_text and semantic_text if semantic_text exists in the schema.
18. When multiple topic LIKE conditions are used with OR, ALWAYS wrap them inside one parenthesized WHERE block.
19. For non-aggregation transaction search queries, include a meaningful ORDER BY:
    - ORDER BY operation_date DESC for recent / latest queries
    - ORDER BY amount_kzt DESC for largest / biggest queries
20. For topic-based transaction search queries, prefer returning:
    tx_id, operation_date, amount_kzt, direction, purpose_text, payer_name, receiver_name
21. For aggregation queries, return only the columns needed for the summary.
22. Never use SELECT *.
"""

    if include_semantic:
        rules += """\
23. Semantic ranking is available:
    - For broad semantic exploration queries, you may use ORDER BY semantic_embedding <-> :query_embedding
    - Do not use semantic ranking instead of explicit structured filters
24. If topic + structured constraints are present, prefer hybrid SQL:
    WHERE structured_filters AND topic_filters
    ORDER BY semantic_embedding <-> :query_embedding
"""

    return rules


def build_prompt(plan: QueryPlan, *, include_semantic: bool) -> str:
    blocks: list[str] = []

    blocks.append(_SYSTEM_ROLE.strip())
    blocks.append("─" * 60)
    blocks.append(schema_prompt_block(include_semantic=include_semantic))
    blocks.append("─" * 60)
    blocks.append(_sql_rules(include_semantic).strip())
    blocks.append("─" * 60)

    blocks.append("DETECTED FILTERS (from the question):")
    blocks.append(plan.entities.as_text())
    if plan.entities.top_n:
        blocks.append(f"→ this is a top-{plan.entities.top_n} ranking query")
    if plan.entities.semantic_topic:
        blocks.append(
            f"→ semantic topic detected: {plan.entities.semantic_topic}. "
            "Use exact topic filters when the user asks for explicit category/keyword matching."
        )
    blocks.append("─" * 60)

    if plan.context.sample_values:
        blocks.append("SAMPLE VALUES FROM THE DATABASE (relevant to this question):")
        blocks.append(plan.context.sample_values_text())

    if plan.context.similar_examples:
        blocks.append("")
        blocks.append("SIMILAR SOLVED QUERIES (learn the SQL pattern):")
        blocks.append(plan.context.examples_text())

    blocks.append("─" * 60)
    blocks.append(f"USER QUESTION:\n{plan.question}")
    blocks.append("")
    blocks.append("Return the SQL query only — no markdown, no explanation.")
    return "\n\n".join(blocks)
