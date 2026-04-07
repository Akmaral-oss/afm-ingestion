from __future__ import annotations

"""
SQL Validator — safety guardrails.

Checks the generated SQL before execution.
Raises SQLValidationError for any violation.
"""

import re

from .schema_registry import NL_VIEW, ALLOWED_COLUMNS


class SQLValidationError(Exception):
    pass


_FORBIDDEN = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|EXEC|EXECUTE)\b",
    re.IGNORECASE,
)

_DANGEROUS_PATTERNS = [
    re.compile(r";\s*\w", re.IGNORECASE),     # multiple statements
    re.compile(r"--"),                          # SQL comment injection
    re.compile(r"/\*"),                         # block comment injection
    re.compile(r"\bpg_\w+", re.IGNORECASE),    # pg_* system functions
    re.compile(r"\bINFORMATION_SCHEMA\b", re.IGNORECASE),
]


def validate_sql(sql: str) -> None:
    """
    Raises SQLValidationError if the SQL is unsafe or violates schema rules.
    Call before every execution.
    """
    s = sql.strip()

    if not s:
        raise SQLValidationError("Empty SQL")

    # must start with SELECT
    if not re.match(r"\s*SELECT\b", s, re.IGNORECASE):
        raise SQLValidationError("Only SELECT statements are allowed")

    # forbidden keywords
    m = _FORBIDDEN.search(s)
    if m:
        raise SQLValidationError(f"Forbidden keyword: {m.group()}")

    # dangerous patterns
    for pat in _DANGEROUS_PATTERNS:
        if pat.search(s):
            raise SQLValidationError(f"Dangerous SQL pattern: {pat.pattern}")

    # must reference the allowed view
    if NL_VIEW not in s.lower():
        raise SQLValidationError(
            f"SQL must reference {NL_VIEW}. Got: {s[:200]}"
        )

    # must not reference raw tables directly
    forbidden_tables = [
        "afm.transactions_core",
        "afm.statements",
        "afm.raw_files",
        "afm.format_registry",
        "afm.transactions_ext",
        "afm.field_discovery_log",
    ]
    for t in forbidden_tables:
        if t in s.lower():
            raise SQLValidationError(
                f"Direct access to raw table '{t}' is not allowed. Use {NL_VIEW}"
            )

    # LIMIT required for non-aggregation queries
    has_group_by = bool(re.search(r"\bGROUP\s+BY\b", s, re.IGNORECASE))
    has_limit = bool(re.search(r"\bLIMIT\s+\d+", s, re.IGNORECASE))
    if not has_group_by and not has_limit:
        raise SQLValidationError(
            "LIMIT is required for non-aggregation queries"
        )
