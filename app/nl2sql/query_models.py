from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Filter:
    field: str
    op: str
    value: Any


@dataclass
class QueryEntities:
    amount: Optional[Filter] = None
    date_range: Optional[Filter] = None
    direction: Optional[Filter] = None
    currency: Optional[Filter] = None
    source_bank: Optional[Filter] = None
    top_n: Optional[int] = None
    semantic_topic: Optional[str] = None

    def to_list(self) -> List[Filter]:
        out: List[Filter] = []
        for item in (
            self.amount,
            self.date_range,
            self.direction,
            self.currency,
            self.source_bank,
        ):
            if item is not None:
                out.append(item)
        return out

    def as_text(self) -> str:
        parts: List[str] = []
        for item in self.to_list():
            parts.append(f"{item.field} {item.op} {item.value}")
        if self.top_n:
            parts.append(f"TOP {self.top_n}")
        if self.semantic_topic:
            parts.append(f'semantic_topic: "{self.semantic_topic}"')
        return "\n".join(parts) if parts else "(none detected)"


@dataclass
class RetrievedContext:
    sample_values: List[str] = field(default_factory=list)
    similar_examples: List[Dict[str, str]] = field(default_factory=list)

    def sample_values_text(self) -> str:
        return "\n".join(f"- {value}" for value in self.sample_values) or "(none)"

    def examples_text(self) -> str:
        if not self.similar_examples:
            return "(none)"

        lines: List[str] = []
        for example in self.similar_examples:
            lines.append(f"Q: {example['nl']}")
            lines.append(f"SQL:\n{example['sql']}")
            lines.append("")
        return "\n".join(lines)


@dataclass
class QueryPlan:
    question: str
    entities: QueryEntities
    context: RetrievedContext
    query_embedding: Optional[Any] = None
