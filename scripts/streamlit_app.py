#!/usr/bin/env python3
from __future__ import annotations

import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st  # pyright: ignore[reportMissingImports]
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import Settings, load_settings_from_env
from app.db.engine import make_engine
from app.db.schema import ensure_schema
from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
from app.nl2sql.entity_extractor import extract_entities
from app.nl2sql.prompt_builder import build_prompt
from app.nl2sql.query_executor import QueryExecutor
from app.nl2sql.query_models import QueryPlan
from app.nl2sql.schema_registry import NL_VIEW, schema_prompt_block
from app.nl2sql.semantic_retriever import SemanticRetriever
from app.nl2sql.sql_generator import OllamaBackend, SQLGenerator
from app.nl2sql.sql_repair import SQLRepair
from app.nl2sql.sql_validator import SQLValidationError, validate_sql


def _init_session_state() -> None:
    if "env_file" not in st.session_state:
        st.session_state.env_file = ".env"
    if "runtime_initialized" not in st.session_state:
        st.session_state.runtime_initialized = False
    if "runtime" not in st.session_state:
        st.session_state.runtime = None
    if "settings" not in st.session_state:
        st.session_state.settings = None
    if "runtime_error" not in st.session_state:
        st.session_state.runtime_error = None
    if "max_attempts" not in st.session_state:
        st.session_state.max_attempts = 3
    if "show_reasoning" not in st.session_state:
        st.session_state.show_reasoning = True
    if "query_history" not in st.session_state:
        st.session_state.query_history = []


def _style_page() -> None:
    st.set_page_config(page_title="AFM NL2SQL Agent", page_icon="🤖", layout="wide")
    st.markdown(
        """
<style>
@import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;700;800&display=swap');

:root {
    --afm-ink: #0f172a;
    --afm-muted: #475569;
    --afm-brand: #0b5d57;
    --afm-brand-soft: #d8f4ef;
    --afm-sky: #1d4ed8;
    --afm-surface: #ffffff;
    --afm-border: #dbe6ef;
}

.main .block-container {
    padding-top: 1.2rem;
    max-width: 1200px;
}

.stApp {
    font-family: 'Manrope', 'Segoe UI', sans-serif;
    color: var(--afm-ink);
    background:
      radial-gradient(900px 460px at 80% -20%, rgba(29, 78, 216, 0.14), transparent 65%),
      radial-gradient(850px 440px at -10% -10%, rgba(11, 93, 87, 0.12), transparent 60%),
      linear-gradient(180deg, #f8fbfd 0%, #f2f7fc 100%);
}

.afm-hero {
    background: linear-gradient(140deg, rgba(11, 93, 87, 0.9), rgba(29, 78, 216, 0.88));
    border-radius: 18px;
    padding: 1.2rem 1.3rem;
    margin-bottom: 1.1rem;
    box-shadow: 0 14px 36px rgba(13, 26, 40, 0.16);
}

.afm-hero h2 {
    color: #eff8ff;
    margin: 0;
    font-size: 1.35rem;
    letter-spacing: 0.2px;
}

.afm-hero p {
    margin: 0.35rem 0 0 0;
    color: rgba(239, 248, 255, 0.9);
    font-size: 0.94rem;
}

.afm-chip-row {
    display: flex;
    gap: 0.55rem;
    flex-wrap: wrap;
    margin-top: 0.75rem;
}

.afm-chip {
    border: 1px solid rgba(255, 255, 255, 0.35);
    color: #f1f8ff;
    padding: 0.2rem 0.55rem;
    border-radius: 999px;
    font-size: 0.78rem;
}

.afm-card {
    background: var(--afm-surface);
    border: 1px solid var(--afm-border);
    border-radius: 14px;
    padding: 0.85rem 0.95rem;
    box-shadow: 0 8px 18px rgba(20, 34, 48, 0.06);
    margin-bottom: 0.7rem;
}

.afm-card .label {
    color: var(--afm-muted);
    font-size: 0.8rem;
}

.afm-card .value {
    color: var(--afm-ink);
    font-weight: 800;
    font-size: 1.05rem;
}

[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #ffffff 0%, #f3f8fc 100%);
    border-right: 1px solid #dce8f2;
}

[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
    color: var(--afm-ink);
}
</style>
        """,
        unsafe_allow_html=True,
    )


def _load_runtime(env_file: Optional[str]) -> Tuple[Dict[str, Any], Settings]:
    settings = load_settings_from_env(env_file)
    if not settings.pg_dsn:
        raise ValueError("AFM_PG_DSN is empty. Set it in .env or pass another env file.")

    engine = make_engine(settings.pg_dsn)
    ensure_schema(engine)

    embedder = EmbeddingBackend(
        settings.embedding_model_path,
        provider=settings.embedding_provider,
        ollama_base_url=settings.embedding_base_url,
        ollama_timeout_s=settings.embedding_timeout_s,
    )

    llm_backend = OllamaBackend(
        model=settings.llm_model_name,
        base_url=settings.llm_base_url,
        timeout_s=settings.llm_timeout_s,
    )

    generator = SQLGenerator(llm_backend, max_new_tokens=settings.llm_max_new_tokens)
    repair = SQLRepair(generator)
    retriever = SemanticRetriever(engine, embedder)
    executor = QueryExecutor(engine)

    runtime = {
        "engine": engine,
        "embedder": embedder,
        "generator": generator,
        "repair": repair,
        "retriever": retriever,
        "executor": executor,
    }
    return runtime, settings


def _initialize_runtime_if_needed(force: bool = False) -> None:
    needs_init = force or not st.session_state.runtime_initialized or st.session_state.runtime is None
    if not needs_init:
        return

    st.session_state.runtime_error = None
    with st.spinner("Preparing NL2SQL runtime (database, embeddings, model)..."):
        try:
            runtime, settings = _load_runtime(st.session_state.env_file)
            st.session_state.runtime = runtime
            st.session_state.settings = settings
            st.session_state.runtime_initialized = True
        except Exception as exc:
            st.session_state.runtime = None
            st.session_state.settings = None
            st.session_state.runtime_initialized = False
            st.session_state.runtime_error = str(exc)


def _get_live_schema(runtime: Dict[str, Any]) -> str:
    engine = runtime["engine"]
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'afm' AND table_name = 'transactions_nl_view'
                    ORDER BY ordinal_position
                    """
                )
            ).fetchall()
        if not rows:
            return schema_prompt_block()
        lines = [f"View: {NL_VIEW}", "", "Columns:"]
        for c, t in rows:
            lines.append(f"  {c}  — {t}")
        return "\n".join(lines)
    except Exception:
        return schema_prompt_block()


def _understanding_text(question: str, entities) -> str:
    parts = [f"Question intent: {question}"]
    if entities.to_list() or entities.top_n or entities.semantic_topic:
        parts.append("Detected entities:")
        parts.append(entities.as_text())
    else:
        parts.append("Detected entities: none")
    return "\n".join(parts)


def _plan_text(entities, sample_count: int, example_count: int) -> str:
    mode = "lexical + structured"
    if entities.semantic_topic:
        mode = "hybrid lexical + semantic ranking"

    lines = [
        f"Retrieval mode: {mode}",
        f"Context sample values: {sample_count}",
        f"Similar NL2SQL examples: {example_count}",
        "Generate SQL, validate, execute, and repair on failure.",
    ]
    return "\n".join(lines)


def execute_query_with_retry(
    runtime: Dict[str, Any],
    user_question: str,
    max_attempts: int = 3,
) -> Tuple[Optional[pd.DataFrame], str, List[Dict[str, Any]]]:
    embedder = runtime["embedder"]
    generator = runtime["generator"]
    repair = runtime["repair"]
    retriever = runtime["retriever"]
    executor = runtime["executor"]

    entities = extract_entities(user_question)

    query_embedding = None
    if embedder.enabled:
        embed_text = entities.semantic_topic or user_question
        query_embedding = embedder.embed([embed_text])[0]

    context = retriever.retrieve(user_question, semantic_topic=entities.semantic_topic)
    plan = QueryPlan(
        question=user_question,
        entities=entities,
        context=context,
        query_embedding=query_embedding,
    )
    prompt = build_prompt(plan)

    attempt_history: List[Dict[str, Any]] = []
    sql = generator.generate(prompt)

    for attempt_number in range(1, max_attempts + 1):
        validation = {"warnings": []}
        error_text = None
        diagnosis = None

        try:
            validate_sql(sql)
        except SQLValidationError as exc:
            error_text = str(exc)
            diagnosis = "Validator rejected SQL before execution."
            attempt_history.append(
                {
                    "attempt_number": attempt_number,
                    "understanding": _understanding_text(user_question, entities),
                    "plan": _plan_text(
                        entities,
                        sample_count=len(context.sample_values),
                        example_count=len(context.similar_examples),
                    ),
                    "sql": sql,
                    "validation": validation,
                    "error": error_text,
                    "diagnosis": diagnosis,
                }
            )
            if attempt_number < max_attempts:
                sql = repair.repair(sql, error_text)
                continue
            return None, sql, attempt_history

        try:
            rows = executor.execute(
                sql,
                query_embedding=query_embedding if ":query_embedding" in sql else None,
            )
            df = pd.DataFrame(rows)
            attempt_history.append(
                {
                    "attempt_number": attempt_number,
                    "understanding": _understanding_text(user_question, entities),
                    "plan": _plan_text(
                        entities,
                        sample_count=len(context.sample_values),
                        example_count=len(context.similar_examples),
                    ),
                    "sql": sql,
                    "validation": validation,
                    "error": None,
                    "diagnosis": None,
                }
            )
            return df, sql, attempt_history
        except Exception as exc:
            error_text = str(exc)
            diagnosis = "Execution failed; attempting SQL correction based on DB error."
            attempt_history.append(
                {
                    "attempt_number": attempt_number,
                    "understanding": _understanding_text(user_question, entities),
                    "plan": _plan_text(
                        entities,
                        sample_count=len(context.sample_values),
                        example_count=len(context.similar_examples),
                    ),
                    "sql": sql,
                    "validation": validation,
                    "error": error_text,
                    "diagnosis": diagnosis,
                }
            )
            if attempt_number < max_attempts:
                sql = repair.repair(sql, error_text)
                continue

    return None, sql, attempt_history


def format_natural_language_response(question: str, result_df: pd.DataFrame, final_sql: str) -> str:
    if result_df is None or result_df.empty:
        return "No rows matched the request. Try relaxing filters or broadening keywords."

    row_count = len(result_df)
    columns = ", ".join(result_df.columns[:8])

    preview = []
    for _, row in result_df.head(3).iterrows():
        parts = []
        if "operation_date" in row and pd.notna(row.get("operation_date")):
            parts.append(f"date={row.get('operation_date')}")
        if "amount_kzt" in row and pd.notna(row.get("amount_kzt")):
            parts.append(f"amount_kzt={row.get('amount_kzt')}")
        if "purpose_text" in row and pd.notna(row.get("purpose_text")):
            parts.append(f"purpose={str(row.get('purpose_text'))[:80]}")
        if parts:
            preview.append("- " + "; ".join(parts))

    lines = [
        f"For question: **{question}**",
        f"Returned **{row_count}** rows.",
        f"Main columns: {columns}",
    ]

    if preview:
        lines.append("\nTop matches:")
        lines.extend(preview)

    lines.append("\nSQL was generated and validated through the NL2SQL agent pipeline.")
    lines.append(f"\nFinal SQL length: {len(final_sql)} chars")
    return "\n".join(lines)


def _runtime_status_card(label: str, value: str) -> None:
    st.markdown(
        f"""
<div class=\"afm-card\">
  <div class=\"label\">{label}</div>
  <div class=\"value\">{value}</div>
</div>
        """,
        unsafe_allow_html=True,
    )


def _render_sidebar() -> None:
    st.header("Control Panel")

    new_env = st.text_input("Env file", value=st.session_state.env_file)
    if new_env != st.session_state.env_file:
        st.session_state.env_file = new_env
        _initialize_runtime_if_needed(force=True)

    st.session_state.max_attempts = st.slider(
        "Retry budget",
        min_value=1,
        max_value=5,
        value=st.session_state.max_attempts,
        help="How many correction rounds are allowed if SQL fails validation or execution.",
    )
    st.session_state.show_reasoning = st.checkbox(
        "Show reasoning",
        value=st.session_state.show_reasoning,
    )

    c1, c2 = st.columns(2)
    with c1:
        reload_clicked = st.button("Reload", use_container_width=True, type="primary")
    with c2:
        clear_clicked = st.button("Clear", use_container_width=True)

    if reload_clicked:
        _initialize_runtime_if_needed(force=True)
        st.rerun()

    if clear_clicked:
        st.session_state.query_history = []
        st.rerun()

    st.markdown("---")
    st.subheader("Runtime")
    if st.session_state.runtime is not None and st.session_state.settings is not None:
        settings: Settings = st.session_state.settings
        st.success("Connected")
        st.caption(f"View: {NL_VIEW}")
        st.caption(f"LLM: {settings.llm_model_name}")
        st.caption(
            f"Embeddings: {settings.embedding_provider} / {settings.embedding_model_path}"
        )

        with st.expander("Schema", expanded=False):
            st.code(_get_live_schema(st.session_state.runtime), language="text")

        with st.expander("Settings", expanded=False):
            safe_settings = asdict(settings)
            safe_settings["pg_dsn"] = "***hidden***"
            st.json(safe_settings)
    elif st.session_state.runtime_error:
        st.error("Initialization failed")
        st.code(st.session_state.runtime_error, language="text")
    else:
        st.info("Starting runtime...")

    st.markdown("---")
    st.subheader("Starter Prompts")
    st.markdown(
        """
- платежи по займам больше 5 млн за 2024
- топ 10 получателей по сумме за 2024
- входящие переводы kaspi за 2023
- операции по депозитам больше 1 млн
        """
    )


def _render_header() -> None:
    st.markdown(
        """
<div class="afm-hero">
  <h2>Financial Query Studio</h2>
  <p>Self-correcting NL2SQL agent for AFM transactions, powered by Ollama and vector retrieval.</p>
  <div class="afm-chip-row">
    <div class="afm-chip">Safe SQL Guardrails</div>
    <div class="afm-chip">Semantic + Lexical Retrieval</div>
    <div class="afm-chip">Auto Repair Loop</div>
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )


def _render_runtime_summary() -> None:
    col1, col2, col3 = st.columns(3)
    settings: Optional[Settings] = st.session_state.settings

    with col1:
        _runtime_status_card(
            "Runtime",
            "Ready" if st.session_state.runtime is not None else "Unavailable",
        )
    with col2:
        llm_name = settings.llm_model_name if settings else "n/a"
        _runtime_status_card("LLM Model", llm_name)
    with col3:
        emb = settings.embedding_model_path if settings else "n/a"
        _runtime_status_card("Embedding Model", str(emb))


def _render_result_panels(
    question: str,
    result_df: Optional[pd.DataFrame],
    final_sql: str,
    attempt_history: List[Dict[str, Any]],
) -> None:
    if result_df is not None:
        st.success("Query executed successfully.")
        tabs = st.tabs(["Answer", "Data", "SQL", "Reasoning"])

        with tabs[0]:
            st.markdown(format_natural_language_response(question, result_df, final_sql))

        with tabs[1]:
            st.dataframe(result_df, use_container_width=True)

        with tabs[2]:
            st.code(final_sql, language="sql")

        with tabs[3]:
            if not st.session_state.show_reasoning:
                st.info("Enable 'Show reasoning' in sidebar to inspect attempts.")
            else:
                for i, attempt in enumerate(attempt_history):
                    if i < len(attempt_history) - 1:
                        st.error(f"Attempt {attempt['attempt_number']} failed")
                    else:
                        st.success(f"Attempt {attempt['attempt_number']} final")

                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown("**Understand**")
                        st.info(attempt["understanding"])
                    with c2:
                        st.markdown("**Plan**")
                        st.info(attempt["plan"])

                    st.markdown("**Generated SQL**")
                    st.code(attempt["sql"], language="sql")

                    if attempt["error"]:
                        st.error(f"Error: {attempt['error']}")
                    if attempt["diagnosis"]:
                        st.warning(attempt["diagnosis"])

                    if i < len(attempt_history) - 1:
                        st.markdown("---")

        st.session_state.query_history.append(
            {
                "question": question,
                "rows": int(len(result_df)),
                "attempts": len(attempt_history),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        return

    st.error("Query failed after all attempts.")
    if st.session_state.show_reasoning and attempt_history:
        with st.expander("Attempt details", expanded=True):
            for attempt in attempt_history:
                st.error(f"Attempt {attempt['attempt_number']}")
                st.code(attempt["sql"], language="sql")
                if attempt["error"]:
                    st.error(f"Error: {attempt['error']}")
                if attempt["diagnosis"]:
                    st.warning(attempt["diagnosis"])
                st.markdown("---")


def main() -> None:
    _style_page()
    _init_session_state()
    _initialize_runtime_if_needed()

    _render_header()

    with st.sidebar:
        _render_sidebar()

    if st.session_state.runtime is None:
        st.error("Runtime is not ready.")
        if st.session_state.runtime_error:
            st.code(st.session_state.runtime_error, language="text")
        st.info("Check .env values and click Reload in the sidebar.")
        st.stop()

    _render_runtime_summary()

    st.markdown("### Ask in natural language")
    col1, col2 = st.columns([4, 1])

    with col1:
        user_question = st.text_area(
            "Question",
            placeholder="e.g., платежи по займам больше 5 млн за 2024",
            height=90,
            key="question_input",
        )

    with col2:
        st.markdown("\n\n")
        run_clicked = st.button("Run Query", type="primary", use_container_width=True)
        sample_clicked = st.button("Use Sample", use_container_width=True)

    if sample_clicked:
        st.session_state.question_input = "платежи по займам больше 5 млн за 2024"
        st.rerun()

    if run_clicked:
        if not user_question.strip():
            st.warning("Please enter a question.")
            st.stop()

        max_attempts = st.session_state.max_attempts

        with st.spinner("🧠 Agent is generating and validating SQL..."):
            result_df, final_sql, attempt_history = execute_query_with_retry(
                st.session_state.runtime,
                user_question.strip(),
                max_attempts=max_attempts,
            )

        _render_result_panels(
            user_question.strip(),
            result_df,
            final_sql,
            attempt_history,
        )

    if st.session_state.query_history:
        st.markdown("---")
        st.markdown("### Query History")
        history_df = pd.DataFrame(st.session_state.query_history)
        st.dataframe(history_df, use_container_width=True)


if __name__ == "__main__":
    main()
