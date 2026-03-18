from __future__ import annotations

import re

from .sql_generator import SQLGenerator

_REPAIR_TEMPLATE = """\
The following PostgreSQL SQL query failed.

--- ORIGINAL SQL ---
{sql}

--- ERROR ---
{error}

--- INSTRUCTIONS ---
Fix the SQL so it:
1. Uses only the view afm.transactions_nl_view
2. Is a valid SELECT statement
3. Includes LIMIT unless GROUP BY is present
4. Does not reference any raw tables

Return the corrected SQL only — no markdown, no explanation.
"""


class SQLRepair:
    def __init__(self, generator: SQLGenerator):
        self.generator = generator

    def repair(self, sql: str, error: str) -> str:
        heuristic_sql = _apply_postgres_fixes(sql)
        if heuristic_sql != sql:
            return heuristic_sql

        prompt = _REPAIR_TEMPLATE.format(sql=sql, error=error)
        return self.generator.generate(prompt)


def _apply_postgres_fixes(sql: str) -> str:
    fixed = sql
    fixed = re.sub(
        r"\bYEAR\s*\(\s*([^)]+?)\s*\)",
        r"EXTRACT(YEAR FROM \1)",
        fixed,
        flags=re.IGNORECASE,
    )
    fixed = re.sub(
        r"\bMONTH\s*\(\s*([^)]+?)\s*\)",
        r"EXTRACT(MONTH FROM \1)",
        fixed,
        flags=re.IGNORECASE,
    )
    fixed = re.sub(
        r"\bDAY\s*\(\s*([^)]+?)\s*\)",
        r"EXTRACT(DAY FROM \1)",
        fixed,
        flags=re.IGNORECASE,
    )
    fixed = re.sub(
        r"\bIFNULL\s*\(",
        "COALESCE(",
        fixed,
        flags=re.IGNORECASE,
    )
    return fixed
