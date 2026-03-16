"""
tests/test_nl2sql.py

Unit tests for the NL→SQL layer.
Run with:  pytest tests/test_nl2sql.py -v
"""
from __future__ import annotations

import pytest

from app.nl2sql.entity_extractor import extract_entities
from app.nl2sql.prompt_builder import build_prompt
from app.nl2sql.query_models import QueryEntities, QueryPlan, RetrievedContext
from app.nl2sql.schema_registry import NL_VIEW
from app.nl2sql.sql_validator import SQLValidationError, validate_sql


# ── entity extractor ──────────────────────────────────────────────────────────

class TestEntityExtractor:

    def test_amount_mln(self):
        e = extract_entities("платежи больше 5 млн")
        assert e.amount is not None
        assert e.amount.value == 5_000_000
        assert e.amount.op == ">"

    def test_amount_tys(self):
        e = extract_entities("суммы менее 500 тыс")
        assert e.amount is not None
        assert e.amount.value == 500_000
        assert e.amount.op == "<"

    def test_year(self):
        e = extract_entities("за 2024 год")
        assert e.date_range is not None
        assert e.date_range.value[0].year == 2024
        assert e.date_range.value[1].year == 2024

    def test_direction_credit(self):
        e = extract_entities("входящие переводы")
        assert e.direction is not None
        assert e.direction.value == "credit"

    def test_direction_debit(self):
        e = extract_entities("исходящие платежи kaspi")
        assert e.direction is not None
        assert e.direction.value == "debit"
        assert e.source_bank is not None
        assert e.source_bank.value == "kaspi"

    def test_semantic_loan(self):
        e = extract_entities("операции по займам")
        assert e.semantic_topic is not None
        assert "займ" in e.semantic_topic

    def test_semantic_tax(self):
        e = extract_entities("налоговые выплаты за 2023")
        assert e.semantic_topic is not None
        assert "налог" in e.semantic_topic

    def test_top_n(self):
        e = extract_entities("топ 10 получателей по сумме")
        assert e.top_n == 10

    def test_no_entities(self):
        e = extract_entities("покажи все транзакции")
        assert e.amount is None
        assert e.date_range is None


# ── sql validator ─────────────────────────────────────────────────────────────

class TestSQLValidator:

    def _valid_sql(self):
        return f"SELECT tx_id, amount_kzt FROM {NL_VIEW} LIMIT 100"

    def test_valid(self):
        validate_sql(self._valid_sql())   # no exception

    def test_no_select(self):
        with pytest.raises(SQLValidationError, match="Only SELECT"):
            validate_sql(f"UPDATE {NL_VIEW} SET amount_kzt = 0")

    def test_forbidden_drop(self):
        with pytest.raises(SQLValidationError, match="Forbidden"):
            validate_sql(f"DROP TABLE {NL_VIEW}")

    def test_wrong_table(self):
        with pytest.raises(SQLValidationError, match="must reference"):
            validate_sql("SELECT * FROM afm.transactions_core LIMIT 10")

    def test_missing_limit(self):
        with pytest.raises(SQLValidationError, match="LIMIT"):
            validate_sql(f"SELECT * FROM {NL_VIEW}")

    def test_aggregation_no_limit_ok(self):
        sql = (
            f"SELECT receiver_name, SUM(amount_kzt) FROM {NL_VIEW} "
            "GROUP BY receiver_name ORDER BY 2 DESC"
        )
        validate_sql(sql)   # should not raise

    def test_multiple_statements(self):
        with pytest.raises(SQLValidationError):
            validate_sql(f"SELECT * FROM {NL_VIEW} LIMIT 10; DROP TABLE afm.raw_files")


# ── prompt builder ────────────────────────────────────────────────────────────

class TestPromptBuilder:

    def _make_plan(self, question: str = "платежи по займам") -> QueryPlan:
        return QueryPlan(
            question=question,
            entities=extract_entities(question),
            context=RetrievedContext(
                sample_values=["погашение займа", "возврат долга"],
                similar_examples=[
                    {"nl": "платежи по кредитам", "sql": f"SELECT * FROM {NL_VIEW} ORDER BY semantic_embedding <-> :query_embedding LIMIT 50"},
                ],
            ),
        )

    def test_contains_view(self):
        prompt = build_prompt(self._make_plan())
        assert NL_VIEW in prompt

    def test_contains_rules(self):
        prompt = build_prompt(self._make_plan())
        assert "SELECT" in prompt
        assert "LIMIT" in prompt

    def test_contains_retrieved_samples(self):
        prompt = build_prompt(self._make_plan())
        assert "погашение займа" in prompt

    def test_contains_similar_example(self):
        prompt = build_prompt(self._make_plan())
        assert "платежи по кредитам" in prompt

    def test_contains_question(self):
        q = "операции по займам больше 5 млн"
        prompt = build_prompt(self._make_plan(q))
        assert q in prompt
