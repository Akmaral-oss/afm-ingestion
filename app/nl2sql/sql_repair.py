from __future__ import annotations

"""
SQL Repair.

When the generated SQL fails validation or execution, sends the SQL + error
back to the LLM with a targeted repair prompt.  Tries up to `max_attempts`.
"""

import logging

from .sql_generator import SQLGenerator

log = logging.getLogger(__name__)

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
    def __init__(self, generator: SQLGenerator, max_attempts: int = 2):
        self.generator = generator
        self.max_attempts = max_attempts

    def repair(self, sql: str, error: str) -> str:
        prompt = _REPAIR_TEMPLATE.format(sql=sql, error=error)
        log.info("Attempting SQL repair. Error: %s", error[:200])
        return self.generator.generate(prompt)

    async def arepair(self, sql: str, error: str) -> str:
        prompt = _REPAIR_TEMPLATE.format(sql=sql, error=error)
        log.info("Attempting async SQL repair. Error: %s", error[:200])
        return await self.generator.agenerate(prompt)
