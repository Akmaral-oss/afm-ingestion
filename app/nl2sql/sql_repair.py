"""
app/nl2sql/sql_repair.py
On SQL validation or execution failure, re-prompts the LLM with the error
text to get a corrected query. Retries up to max_attempts times.
"""
from __future__ import annotations
import logging
from .sql_generator import SQLGenerator

log = logging.getLogger(__name__)

_REPAIR_TEMPLATE = """\
The following PostgreSQL SQL query failed with the error shown below.

--- ORIGINAL SQL ---
{sql}

--- ERROR ---
{error}

--- INSTRUCTIONS ---
Fix the SQL so that:
1. It only queries the view afm.transactions_nl_view
2. It is a valid SELECT statement
3. It includes LIMIT unless it uses GROUP BY
4. It does NOT reference any raw tables
5. COLUMN NAME: bank filter must use source_bank, NOT 'source' or 'bank'.
   CORRECT: WHERE source_bank = 'kaspi'
6. If the error is about direction='debit' on Halyk data, use:
   (direction = 'debit' OR direction IS NULL)
7. For vector search use: ORDER BY semantic_embedding <-> :query_embedding
8. Do NOT use LIKE on semantic_text — only on purpose_text and raw_note
9. BRACKETS: when combining date filter AND LIKE filters, wrap all LIKE in one block:
   WHERE operation_date BETWEEN '...' AND '...' AND (LIKE ... OR LIKE ...)
10. NULL AMOUNTS: if using SUM(amount_kzt) add WHERE amount_kzt IS NOT NULL

Return the corrected SQL only — no markdown, no explanation.
"""


class SQLRepair:
    def __init__(self, generator: SQLGenerator, max_attempts: int = 2):
        self.generator = generator
        self.max_attempts = max_attempts

    def repair(self, sql: str, error: str) -> str:
        prompt = _REPAIR_TEMPLATE.format(sql=sql, error=error[:500])
        log.info("Attempting SQL repair. Error: %s", error[:200])
        for attempt in range(1, self.max_attempts + 1):
            try:
                repaired = self.generator.generate(prompt)
                log.info("Repair attempt %d succeeded", attempt)
                return repaired
            except Exception as exc:
                log.warning("Repair attempt %d failed: %s", attempt, exc)
                if attempt == self.max_attempts:
                    raise
        return sql  # unreachable but satisfies type checkers
