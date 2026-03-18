from __future__ import annotations

"""
SQL Repair.

When the generated SQL fails validation or execution, sends the SQL + error
back to the LLM with a targeted repair prompt.  Tries up to `max_attempts`.

Includes intelligent error context and suggestions.
"""

import logging
import re

from .sql_generator import SQLGenerator

log = logging.getLogger(__name__)

_REPAIR_TEMPLATE = """\
The following PostgreSQL SQL query failed.

--- ORIGINAL SQL ---
{sql}

--- ERROR ---
{error}

--- COMMON FIXES ---
{suggestions}

--- INSTRUCTIONS ---
Fix the SQL so it:
1. Uses only the view afm.transactions_nl_view
2. Is a valid SELECT statement
3. Includes LIMIT unless GROUP BY is present
4. Does not reference any raw tables
5. Does not have duplicate LIKE conditions
6. Does not SELECT semantic_embedding column

Return the corrected SQL only — no markdown, no explanation.
"""


class SQLRepair:
    def __init__(self, generator: SQLGenerator, max_attempts: int = 2):
        self.generator = generator
        self.max_attempts = max_attempts

    def repair(self, sql: str, error: str) -> str:
        suggestions = self._suggest_fixes(sql, error)
        prompt = _REPAIR_TEMPLATE.format(
            sql=sql,
            error=error,
            suggestions=suggestions,
        )
        log.info("Attempting SQL repair. Error: %s", error[:200])
        return self.generator.generate(prompt)

    def _suggest_fixes(self, sql: str, error: str) -> str:
        """Generate contextual fix suggestions based on error type."""
        suggestions = []
        
        error_lower = error.lower()
        
        # Common error patterns
        if "must reference" in error_lower or "afm.transactions_nl_view" in error_lower:
            suggestions.append("- Must use: FROM afm.transactions_nl_view")
        
        if "duplicate" in error_lower or "unique" in error_lower:
            suggestions.append("- Remove duplicate LIKE conditions (same % pattern)")
        
        if "limit" in error_lower:
            if "GROUP BY" not in sql.upper():
                suggestions.append("- Non-aggregation queries must have: ... LIMIT 100")
            else:
                suggestions.append("- GROUP BY queries should still have LIMIT (e.g., LIMIT 20)")
        
        if "semantic_embedding" in error_lower:
            suggestions.append("- semantic_embedding cannot be SELECTed. Use only in ORDER BY")
        
        if "invalid" in error_lower and "column" in error_lower:
            suggestions.append("- Check column names are from allowed list")
            suggestions.append("- Valid columns: tx_id, operation_date, amount_kzt, payer_name, receiver_name, ...")
        
        if "syntax" in error_lower:
            suggestions.append("- Check SQL syntax: parentheses, commas, keywords")
            suggestions.append("- WHERE conditions should be properly parenthesized")
        
        if not suggestions:
            suggestions.append("- Check that all column names are valid in afm.transactions_nl_view")
            suggestions.append("- Check WHERE clause syntax and conditions")
            suggestions.append("- Ensure LIMIT is present for non-GROUP BY queries")
        
        return "\n".join(suggestions)

