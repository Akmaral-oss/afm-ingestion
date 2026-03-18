from __future__ import annotations

import re

from .schema_registry import NL_VIEW


class SQLValidationError(Exception):
    pass


_FORBIDDEN = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|EXEC|EXECUTE)\b",
    re.IGNORECASE,
)
_DANGEROUS_PATTERNS = [
    re.compile(r";\s*\w", re.IGNORECASE),
    re.compile(r"--"),
    re.compile(r"/\*"),
    re.compile(r"\bpg_\w+", re.IGNORECASE),
    re.compile(r"\bINFORMATION_SCHEMA\b", re.IGNORECASE),
]


def ensure_limit(sql: str, *, default_limit: int = 100) -> str:
    normalized = sql.strip()
    if not normalized:
        return sql

    has_group_by = bool(re.search(r"\bGROUP\s+BY\b", normalized, re.IGNORECASE))
    has_limit = bool(re.search(r"\bLIMIT\s+\d+", normalized, re.IGNORECASE))
    if has_group_by or has_limit:
        return normalized

    return normalized.rstrip().rstrip(";") + f"\nLIMIT {default_limit};"


def validate_sql(sql: str, *, allow_semantic: bool) -> None:
    normalized = sql.strip()
    lowered = normalized.lower()

    if not normalized:
        raise SQLValidationError("Empty SQL")
    if not re.match(r"\s*SELECT\b", normalized, re.IGNORECASE):
        raise SQLValidationError("Only SELECT statements are allowed")
    if re.search(r"\bSELECT\s+\*", normalized, re.IGNORECASE):
        raise SQLValidationError("SELECT * is not allowed")

    match = _FORBIDDEN.search(normalized)
    if match:
        raise SQLValidationError(f"Forbidden keyword: {match.group()}")

    for pattern in _DANGEROUS_PATTERNS:
        if pattern.search(normalized):
            raise SQLValidationError(f"Dangerous SQL pattern: {pattern.pattern}")

    if NL_VIEW not in lowered:
        raise SQLValidationError(f"SQL must reference {NL_VIEW}")

    forbidden_tables = [
        "afm.transactions_core",
        "afm.statements",
        "afm.raw_files",
        "afm.format_registry",
        "afm.transactions_ext",
        "afm.field_discovery_log",
    ]
    for table_name in forbidden_tables:
        if table_name in lowered:
            raise SQLValidationError(f"Direct access to raw table '{table_name}' is not allowed")

    if not allow_semantic and (
        "semantic_embedding" in lowered or ":query_embedding" in lowered
    ):
        raise SQLValidationError("Semantic search is not enabled for this runtime")

    has_group_by = bool(re.search(r"\bGROUP\s+BY\b", normalized, re.IGNORECASE))
    has_limit = bool(re.search(r"\bLIMIT\s+\d+", normalized, re.IGNORECASE))
    if not has_group_by and not has_limit:
        raise SQLValidationError("LIMIT is required for non-aggregation queries")
