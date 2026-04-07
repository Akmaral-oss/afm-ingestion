from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Filter:
    """Single SQL filter condition."""
    field: str
    op: str        # "=", ">", "<", ">=", "<=", "between", "ilike", "year"
    value: Any


@dataclass
class QueryEntities:
    """Rule-extracted structured entities from the user question."""
    amount: Optional[Filter] = None
    date_range: Optional[Filter] = None
    direction: Optional[Filter] = None
    currency: Optional[Filter] = None
    source_bank: Optional[Filter] = None
    top_n: Optional[int] = None
    semantic_topic: Optional[str] = None   # e.g. "займ долг кредит"

    def to_list(self) -> List[Filter]:
        out: List[Filter] = []
        for f in (self.amount, self.date_range, self.direction,
                  self.currency, self.source_bank):
            if f is not None:
                out.append(f)
        return out

    def as_text(self) -> str:
        parts: List[str] = []
        for f in self.to_list():
            parts.append(f"{f.field} {f.op} {f.value}")
        if self.top_n:
            parts.append(f"TOP {self.top_n}")
        if self.semantic_topic:
            parts.append(f'semantic_topic: "{self.semantic_topic}"')
        return "\n".join(parts) if parts else "(none detected)"


@dataclass
class RetrievedContext:
    """Dynamic context fetched from the semantic catalog and query history."""
    sample_values: List[str] = field(default_factory=list)
    similar_examples: List[Dict[str, str]] = field(default_factory=list)  # [{nl, sql}]

    def sample_values_text(self) -> str:
        return "\n".join(f"- {v}" for v in self.sample_values) or "(none)"

    def examples_text(self) -> str:
        if not self.similar_examples:
            return "(none)"
        lines: List[str] = []
        for ex in self.similar_examples:
            lines.append(f"Q: {ex['nl']}")
            lines.append(f"SQL:\n{ex['sql']}")
            lines.append("")
        return "\n".join(lines)


@dataclass
class QueryPlan:
    """Fully resolved plan passed to the SQL generator."""
    question: str
    entities: QueryEntities
    context: RetrievedContext
    query_embedding: Optional[Any] = None   # numpy array if semantic intent
