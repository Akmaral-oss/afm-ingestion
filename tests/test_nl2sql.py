"""
tests/test_nl2sql.py
FIX Gap 8: adds tests for SQLRepair, CatalogEntityResolver mock path,
Halyk direction fix, semantic_text composition, and build_semantic_text.
"""
from __future__ import annotations
import re
from unittest.mock import MagicMock, patch
import pytest

from app.nl2sql.entity_extractor import extract_entities
from app.nl2sql.prompt_builder import build_prompt
from app.nl2sql.query_models import QueryEntities, QueryPlan, RetrievedContext
from app.nl2sql.schema_registry import NL_VIEW
from app.nl2sql.sql_validator import SQLValidationError, validate_sql
from app.nl2sql.sql_repair import SQLRepair
from app.nl2sql.query_service import _apply_halyk_direction_fix
from app.semantic.cluster_builder import build_semantic_text


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

    def test_month_year(self):
        e = extract_entities("платежи за январь 2024")
        assert e.date_range is not None
        from datetime import date
        assert e.date_range.value[0] == date(2024, 1, 1)
        assert e.date_range.value[1] == date(2024, 1, 31)

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
        assert "займ" in e.semantic_topic.lower() or "loan" in e.semantic_topic.lower()

    def test_semantic_tax(self):
        e = extract_entities("налоговые выплаты за 2023")
        assert e.semantic_topic is not None

    def test_top_n(self):
        e = extract_entities("топ 10 получателей по сумме")
        assert e.top_n == 10

    def test_no_entities(self):
        e = extract_entities("покажи все транзакции")
        assert e.amount is None
        assert e.date_range is None

    def test_year_not_treated_as_amount(self):
        """Standalone year like 2024 must NOT become an amount filter."""
        e = extract_entities("транзакции за 2024")
        assert e.amount is None
        assert e.date_range is not None

    def test_qualitative_large(self):
        e = extract_entities("крупные транзакции halyk")
        assert e.amount is not None
        assert e.amount.value == 100_000
        assert e.source_bank.value == "halyk"


# ── sql validator ─────────────────────────────────────────────────────────────

class TestSQLValidator:

    def _valid(self):
        return f"SELECT tx_id, amount_kzt FROM {NL_VIEW} LIMIT 100"

    def test_valid(self):
        validate_sql(self._valid())

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
        validate_sql(sql)  # should not raise

    def test_multiple_statements(self):
        with pytest.raises(SQLValidationError):
            validate_sql(f"SELECT * FROM {NL_VIEW} LIMIT 10; DROP TABLE afm.raw_files")

    def test_sql_comment_injection(self):
        with pytest.raises(SQLValidationError):
            validate_sql(f"SELECT * FROM {NL_VIEW} LIMIT 10 -- injected comment")

    def test_pg_system_function(self):
        with pytest.raises(SQLValidationError):
            validate_sql(f"SELECT pg_read_file('/etc/passwd') FROM {NL_VIEW} LIMIT 1")


# ── Halyk NULL-direction fix (Gap 6) ─────────────────────────────────────────

class TestHalykDirectionFix:

    def test_halyk_debit_rewritten(self):
        sql = f"SELECT * FROM {NL_VIEW} WHERE direction = 'debit' AND source_bank = 'halyk' LIMIT 10"
        fixed = _apply_halyk_direction_fix(sql, "halyk")
        assert "(direction = 'debit' OR direction IS NULL)" in fixed
        assert "direction = 'debit'" not in fixed.replace(
            "(direction = 'debit' OR direction IS NULL)", ""
        )

    def test_non_halyk_not_rewritten(self):
        sql = f"SELECT * FROM {NL_VIEW} WHERE direction = 'debit' LIMIT 10"
        fixed = _apply_halyk_direction_fix(sql, "kaspi")
        assert fixed == sql  # unchanged

    def test_no_bank_filter_rewrites(self):
        """Mixed data (no bank filter) should also get the fix."""
        sql = f"SELECT * FROM {NL_VIEW} WHERE direction = 'debit' LIMIT 10"
        fixed = _apply_halyk_direction_fix(sql, None)
        assert "(direction = 'debit' OR direction IS NULL)" in fixed

    def test_already_fixed_no_double_rewrite(self):
        sql = (
            f"SELECT * FROM {NL_VIEW} "
            "WHERE (direction = 'debit' OR direction IS NULL) LIMIT 10"
        )
        fixed = _apply_halyk_direction_fix(sql, "halyk")
        # Should not create nested parentheses
        assert fixed.count("OR direction IS NULL") == 1


# ── semantic_text composition (Gap 4) ────────────────────────────────────────

class TestBuildSemanticText:

    def test_all_fields(self):
        t = build_semantic_text(
            operation_type_raw="исх.док.(дебет)",
            sdp_name=None,
            purpose_text="оплата аренды",
            raw_note="оплата аренды",
        )
        # purpose_text and raw_note are identical — should be deduplicated
        assert t == "исх.док.(дебет) | оплата аренды"

    def test_none_returns_none(self):
        assert build_semantic_text() is None

    def test_purpose_only(self):
        t = build_semantic_text(purpose_text="погашение займа")
        assert t == "погашение займа"

    def test_all_fields_distinct(self):
        t = build_semantic_text(
            operation_type_raw="credit",
            sdp_name="Kaspi Pay",
            purpose_text="зарплата",
            raw_note="зарплата за январь",
        )
        assert "credit" in t
        assert "Kaspi Pay" in t
        assert "зарплата" in t
        assert "зарплата за январь" in t

    def test_whitespace_stripped(self):
        t = build_semantic_text(purpose_text="  оплата  ", raw_note="  ")
        assert t == "оплата"


# ── sql repair (Gap 2) ────────────────────────────────────────────────────────

class TestSQLRepair:

    def _make_repair(self, fixed_sql: str) -> SQLRepair:
        gen = MagicMock()
        gen.generate.return_value = fixed_sql
        return SQLRepair(gen, max_attempts=2)

    def test_repair_returns_llm_output(self):
        fixed = f"SELECT tx_id FROM {NL_VIEW} LIMIT 10"
        repair = self._make_repair(fixed)
        result = repair.repair("SELECT * FROM bad_table", "table not found")
        assert result == fixed

    def test_repair_called_with_error_context(self):
        gen = MagicMock()
        gen.generate.return_value = f"SELECT tx_id FROM {NL_VIEW} LIMIT 10"
        repair = SQLRepair(gen)
        repair.repair("SELECT 1", "syntax error near '1'")
        prompt = gen.generate.call_args[0][0]
        assert "syntax error" in prompt
        assert "SELECT 1" in prompt

    def test_repair_retries_on_exception(self):
        gen = MagicMock()
        gen.generate.side_effect = [
            Exception("timeout"),
            f"SELECT tx_id FROM {NL_VIEW} LIMIT 10",
        ]
        repair = SQLRepair(gen, max_attempts=2)
        result = repair.repair("bad sql", "error")
        assert NL_VIEW in result

    def test_repair_raises_after_max_attempts(self):
        gen = MagicMock()
        gen.generate.side_effect = Exception("always fails")
        repair = SQLRepair(gen, max_attempts=2)
        with pytest.raises(Exception):
            repair.repair("bad sql", "error")


# ── prompt builder ────────────────────────────────────────────────────────────

class TestPromptBuilder:

    def _make_plan(self, question: str = "платежи по займам") -> QueryPlan:
        return QueryPlan(
            question=question,
            entities=extract_entities(question),
            context=RetrievedContext(
                sample_values=["погашение займа", "возврат долга"],
                similar_examples=[
                    {"nl": "платежи по кредитам",
                     "sql": f"SELECT * FROM {NL_VIEW} ORDER BY semantic_embedding <-> :query_embedding LIMIT 50"},
                ],
            ),
        )

    def test_contains_view(self):
        assert NL_VIEW in build_prompt(self._make_plan())

    def test_contains_rules(self):
        prompt = build_prompt(self._make_plan())
        assert "SELECT" in prompt
        assert "LIMIT" in prompt

    def test_contains_retrieved_samples(self):
        assert "погашение займа" in build_prompt(self._make_plan())

    def test_contains_similar_example(self):
        assert "платежи по кредитам" in build_prompt(self._make_plan())

    def test_contains_question(self):
        q = "операции по займам больше 5 млн"
        assert q in build_prompt(self._make_plan(q))

    def test_catalog_context_included(self):
        plan = QueryPlan(
            question="налоги kaspi",
            entities=extract_entities("налоги kaspi"),
            context=RetrievedContext(),
            catalog_context='SEMANTIC CATALOG MATCHES:\n  Cluster 1: "налог / ндс / кпн"',
        )
        prompt = build_prompt(plan)
        assert "налог / ндс" in prompt


# ── config env loading (Gap 5) ────────────────────────────────────────────────

class TestConfig:

    def test_missing_pg_dsn_raises(self):
        with patch.dict("os.environ", {}, clear=True):
            from app.config import load_settings_from_env
            with pytest.raises(ValueError, match="AFM_PG_DSN"):
                load_settings_from_env()

    def test_all_defaults(self):
        with patch.dict("os.environ", {"AFM_PG_DSN": "postgresql://test/db"}):
            from app.config import load_settings_from_env
            s = load_settings_from_env()
            assert s.pg_dsn == "postgresql://test/db"
            assert s.cluster_rebuild_every_n == 500
            assert s.llm_backend == "ollama"
            assert s.api_port == 8000

    def test_custom_values(self):
        env = {
            "AFM_PG_DSN": "postgresql://x/y",
            "AFM_CLUSTER_REBUILD_EVERY_N": "200",
            "AFM_LLM_MODEL": "qwen2.5-coder:14b",
            "AFM_API_PORT": "9000",
        }
        with patch.dict("os.environ", env):
            from app.config import load_settings_from_env
            s = load_settings_from_env()
            assert s.cluster_rebuild_every_n == 200
            assert s.llm_model == "qwen2.5-coder:14b"
            assert s.api_port == 9000
