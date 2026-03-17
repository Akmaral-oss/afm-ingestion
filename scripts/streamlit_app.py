#!/usr/bin/env python3
from __future__ import annotations

import io
import os
import sys
import time
import uuid
from dataclasses import asdict
from datetime import date, datetime
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
from app.nl2sql.sql_generator import SQLGenerator, build_llm_backend
from app.nl2sql.sql_repair import SQLRepair
from app.nl2sql.sql_validator import SQLValidationError, validate_sql

_QUERY_MODE_CONFIG: Dict[str, Dict[str, Any]] = {
    "Strict": {
        "catalog_top_k": 4,
        "history_top_k": 2,
        "prompt_hint": "STRICT MODE: prefer exact lexical and structured filters. Avoid broad semantic expansion.",
    },
    "Balanced": {
        "catalog_top_k": 8,
        "history_top_k": 3,
        "prompt_hint": "BALANCED MODE: use hybrid lexical + semantic retrieval where appropriate.",
    },
    "Broad semantic": {
        "catalog_top_k": 14,
        "history_top_k": 6,
        "prompt_hint": "BROAD SEMANTIC MODE: prioritize semantic retrieval/ranking and use lighter lexical constraints.",
    },
}


def _normalize_cell_for_streamlit(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool, pd.Timestamp, datetime, date)):
        return value
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return str(value)


def _to_display_df(df: pd.DataFrame) -> pd.DataFrame:
    safe_df = df.copy()
    for col in safe_df.columns:
        if safe_df[col].dtype == "object":
            safe_df[col] = safe_df[col].map(_normalize_cell_for_streamlit)
    return safe_df


def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    return _to_display_df(df).to_csv(index=False).encode("utf-8")


def _to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        _to_display_df(df).to_excel(writer, index=False, sheet_name="results")
    return bio.getvalue()


def _init_session_state() -> None:
    defaults = {
        "env_file": ".env",
        "theme_mode": "Dark",
        "runtime_initialized": False,
        "runtime": None,
        "settings": None,
        "runtime_error": None,
        "runtime_status": [],
        "max_attempts": 3,
        "show_reasoning": True,
        "query_mode": "Balanced",
        "max_rows": 1000,
        "query_history": [],
        "chat_messages": [],
        "saved_prompts": [],
        "queued_prompt": "",
        "last_history_id": None,
        "last_question": "",
        "last_generated_sql": "",
        "last_query_embedding": None,
        "sql_editor_text": "",
        "health_report": None,
        "last_result_df": None,
        "last_result_sql": "",
        "last_attempt_history": [],
        "last_result_meta": None,
        "feedback_message": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _style_page() -> None:
    st.set_page_config(page_title="AFM NL2SQL Agent", page_icon="", layout="wide")
    is_dark = str(st.session_state.get("theme_mode", "Dark")).lower() == "dark"

    if is_dark:
        colors = {
            "bg_a": "#0b1221",
            "bg_b": "#08101d",
            "text": "#e2e8f0",
            "muted": "#93a4bc",
            "surface": "rgba(15, 23, 42, 0.78)",
            "surface_strong": "rgba(30, 41, 59, 0.86)",
            "border": "rgba(148, 163, 184, 0.22)",
            "brand": "#22c55e",
            "brand_soft": "rgba(34, 197, 94, 0.18)",
            "accent": "#38bdf8",
            "sidebar": "linear-gradient(180deg, rgba(15, 23, 42, 0.95) 0%, rgba(8, 15, 30, 0.98) 100%)",
        }
    else:
        colors = {
            "bg_a": "#f3f8ff",
            "bg_b": "#e8f2fb",
            "text": "#0f172a",
            "muted": "#475569",
            "surface": "rgba(255, 255, 255, 0.92)",
            "surface_strong": "rgba(255, 255, 255, 0.98)",
            "border": "rgba(15, 23, 42, 0.12)",
            "brand": "#0f766e",
            "brand_soft": "rgba(15, 118, 110, 0.14)",
            "accent": "#2563eb",
            "sidebar": "linear-gradient(180deg, #f7fbff 0%, #edf4fb 100%)",
        }

    css = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;700;800&display=swap');

:root {
    --afm-ink: __TEXT__;
    --afm-muted: __MUTED__;
    --afm-brand: __BRAND__;
    --afm-sky: __ACCENT__;
    --afm-surface: __SURFACE__;
    --afm-surface-strong: __SURFACE_STRONG__;
    --afm-border: __BORDER__;
    --afm-brand-soft: __BRAND_SOFT__;
}

.main .block-container {
    padding-top: 0.9rem;
    padding-bottom: 1.25rem;
    max-width: 1240px;
}

.stApp {
    font-family: 'Manrope', 'Segoe UI', sans-serif;
    color: var(--afm-ink);
    background:
      radial-gradient(900px 460px at 80% -20%, rgba(56, 189, 248, 0.20), transparent 65%),
      radial-gradient(850px 440px at -10% -10%, rgba(34, 197, 94, 0.18), transparent 60%),
      linear-gradient(180deg, __BG_A__ 0%, __BG_B__ 100%);
}

[data-testid="stHeader"] {
    background: transparent;
}

.afm-shell {
    background: var(--afm-surface);
    border: 1px solid var(--afm-border);
    border-radius: 22px;
    padding: 1rem 1rem 0.9rem 1rem;
    backdrop-filter: blur(4px);
    box-shadow: 0 16px 48px rgba(2, 6, 23, 0.12);
    margin-bottom: 1rem;
}

.afm-topbar {
    background: linear-gradient(120deg, rgba(34, 197, 94, 0.18), rgba(56, 189, 248, 0.22));
    border: 1px solid var(--afm-border);
    border-radius: 16px;
    padding: 0.9rem 1rem;
    display: flex;
    justify-content: space-between;
    gap: 0.8rem;
    align-items: center;
    margin-bottom: 0.9rem;
}

.afm-topbar h2 {
    color: var(--afm-ink);
    margin: 0;
    font-size: 1.15rem;
}

.afm-topbar p {
    margin: 0.2rem 0 0 0;
    color: var(--afm-muted);
    font-size: 0.87rem;
}

.afm-chip-row {
    display: flex;
    gap: 0.55rem;
    flex-wrap: wrap;
    justify-content: flex-end;
}

.afm-chip {
    border: 1px solid var(--afm-border);
    color: var(--afm-ink);
    background: var(--afm-surface-strong);
    padding: 0.2rem 0.55rem;
    border-radius: 999px;
    font-size: 0.78rem;
}

.afm-loader {
    border: 1px solid var(--afm-border);
    border-radius: 14px;
    background: var(--afm-surface-strong);
    padding: 0.7rem 0.9rem;
    margin-bottom: 0.8rem;
}

.afm-loader h4 {
    margin: 0;
    color: var(--afm-ink);
    font-size: 0.94rem;
}

.afm-loader p {
    margin: 0.2rem 0 0 0;
    color: var(--afm-muted);
    font-size: 0.78rem;
}

div[data-testid="stMetric"] {
    background: var(--afm-surface-strong);
    border: 1px solid var(--afm-border);
    border-radius: 12px;
    padding: 0.5rem 0.6rem;
}

div[data-testid="stMetricLabel"] {
    color: var(--afm-muted);
}

div[data-testid="stMetricValue"] {
    color: var(--afm-ink);
}

div[data-baseweb="tab-list"] {
    background: transparent;
    gap: 0.35rem;
}

button[data-baseweb="tab"] {
    border-radius: 10px;
    border: 1px solid var(--afm-border) !important;
    background: var(--afm-surface-strong) !important;
    color: var(--afm-ink) !important;
    font-weight: 600;
}

button[data-baseweb="tab"][aria-selected="true"] {
    background: var(--afm-brand-soft) !important;
    border: 1px solid var(--afm-brand) !important;
}

div[data-testid="stDataFrame"] {
    border: 1px solid var(--afm-border);
    border-radius: 12px;
}

.stTextArea textarea,
.stTextInput input {
    border-radius: 12px !important;
    border: 1px solid var(--afm-border) !important;
    background: var(--afm-surface-strong) !important;
    color: var(--afm-ink) !important;
}

[data-testid="stSidebar"] {
    background: __SIDEBAR__;
    border-right: 1px solid var(--afm-border);
}

[data-testid="stSidebar"] .stRadio label,
[data-testid="stSidebar"] .stMarkdown,
[data-testid="stSidebar"] .stCaption,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] p {
    color: var(--afm-ink) !important;
}

.stButton button {
    border-radius: 10px !important;
    border: 1px solid var(--afm-border) !important;
}
</style>
    """

    token_map = {
        "__TEXT__": colors["text"],
        "__MUTED__": colors["muted"],
        "__BRAND__": colors["brand"],
        "__ACCENT__": colors["accent"],
        "__SURFACE__": colors["surface"],
        "__SURFACE_STRONG__": colors["surface_strong"],
        "__BORDER__": colors["border"],
        "__BRAND_SOFT__": colors["brand_soft"],
        "__BG_A__": colors["bg_a"],
        "__BG_B__": colors["bg_b"],
        "__SIDEBAR__": colors["sidebar"],
    }
    for token, value in token_map.items():
        css = css.replace(token, value)

    st.markdown(css, unsafe_allow_html=True)


def _resolve_env_path(env_file: str) -> Path:
    p = Path(env_file)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return p


@st.cache_resource(show_spinner=False)
def _build_runtime_cached(env_file_abs: str, env_mtime: float) -> Tuple[Dict[str, Any], Settings]:
    del env_mtime
    settings = load_settings_from_env(env_file_abs)
    if not settings.pg_dsn:
        raise ValueError("AFM_PG_DSN is empty. Set it in .env or provide another env file.")

    engine = make_engine(settings.pg_dsn)
    ensure_schema(engine)

    embedder = EmbeddingBackend(
        settings.embedding_model_path,
        provider=settings.embedding_provider,
        ollama_base_url=settings.embedding_base_url,
        ollama_timeout_s=settings.embedding_timeout_s,
    )

    llm_backend = build_llm_backend(
        model_name=settings.llm_model_name,
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


def _load_runtime(env_file: Optional[str]) -> Tuple[Dict[str, Any], Settings]:
    env_path = _resolve_env_path(env_file or ".env")
    env_mtime = env_path.stat().st_mtime if env_path.exists() else 0.0
    return _build_runtime_cached(str(env_path), env_mtime)


def _initialize_runtime_if_needed(force: bool = False) -> None:
    needs_init = force or not st.session_state.runtime_initialized or st.session_state.runtime is None
    if not needs_init:
        return

    if force:
        _build_runtime_cached.clear()

    st.session_state.runtime_error = None
    st.session_state.runtime_status = []
    loader_box = st.empty()

    def _render_loading(done_steps: List[str], current_step: str, note: Optional[str] = None) -> None:
        done_html = "".join(f"<li>{s}</li>" for s in done_steps)
        note_html = f"<p>{note}</p>" if note else ""
        loader_box.markdown(
            f"""
<div class="afm-loader">
  <h4>Preparing runtime (database, embeddings, model)</h4>
  <p><strong>Now loading:</strong> {current_step}</p>
  <p><strong>Completed:</strong></p>
  <ul style="margin-top: 0.15rem; margin-bottom: 0.25rem; opacity: 0.92; font-size: 0.78rem;">
    {done_html or '<li>Starting...</li>'}
  </ul>
  {note_html}
</div>
            """,
            unsafe_allow_html=True,
        )

    done: List[str] = []
    _render_loading(done, "Reading .env and resolving configuration")
    try:
        done.append("Environment configuration loaded")
        _render_loading(
            done,
            "Connecting to database, ensuring schema, and creating model backends",
            note="This can take a few seconds on first load.",
        )
        runtime, settings = _load_runtime(st.session_state.env_file)
        st.session_state.runtime = runtime
        st.session_state.settings = settings
        st.session_state.runtime_initialized = True
        st.session_state.runtime_error = None

        llm_name = (settings.llm_model_name or "").strip().lower()
        if llm_name.startswith("gemini") and not os.getenv("GEMINI_API_KEY"):
            done.append("Gemini selected without GEMINI_API_KEY (queries will fail until key is set)")

        done.extend([
            "Database connection ready",
            "Schema check complete",
            "Embedding backend initialized",
            "LLM backend initialized",
        ])
        st.session_state.runtime_status = done
        _render_loading(done, "Finalizing runtime state", note="Runtime is ready.")
        time.sleep(0.15)
        loader_box.empty()
    except Exception as exc:
        st.session_state.runtime = None
        st.session_state.settings = None
        st.session_state.runtime_initialized = False
        error_text = str(exc)
        if "GEMINI_API_KEY" in error_text:
            error_text = (
                "Gemini key was not detected. Add GEMINI_API_KEY to .env or shell env. "
                "The app will still load after key is configured and Reconnect is clicked."
            )
        st.session_state.runtime_error = error_text
        _render_loading(
            done,
            "Initialization failed",
            note=error_text,
        )


@st.cache_data(ttl=300, show_spinner=False)
def _get_live_schema_cached(pg_dsn: str) -> str:
    try:
        engine = make_engine(pg_dsn)
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
            lines.append(f"  {c}   {t}")
        return "\n".join(lines)
    except Exception:
        return schema_prompt_block()


def _get_live_schema(settings: Settings) -> str:
    return _get_live_schema_cached(settings.pg_dsn)


def _run_runtime_health(runtime: Dict[str, Any], settings: Settings, last_error: Optional[str]) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "last_error": last_error,
    }

    engine = runtime["engine"]
    embedder = runtime["embedder"]
    backend = runtime["generator"].backend

    db_status = "ok"
    db_error = None
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        db_status = "fail"
        db_error = str(exc)

    emb_status = "ok" if embedder.enabled else "disabled"
    emb_dim = getattr(embedder, "dim", None)

    llm_backend = type(backend).__name__
    report.update(
        {
            "db": {"status": db_status, "error": db_error},
            "embedding": {
                "status": emb_status,
                "provider": settings.embedding_provider,
                "model": settings.embedding_model_path,
                "dim": emb_dim,
            },
            "llm": {
                "backend": llm_backend,
                "model": settings.llm_model_name,
                "timeout_s": settings.llm_timeout_s,
            },
        }
    )
    return report


def _understanding_text(question: str, entities) -> str:
    parts = [f"Question intent: {question}"]
    if entities.to_list() or entities.top_n or entities.semantic_topic:
        parts.append("Detected entities:")
        parts.append(entities.as_text())
    else:
        parts.append("Detected entities: none")
    return "\n".join(parts)


def _plan_text(entities, sample_count: int, example_count: int, query_mode: str) -> str:
    mode = "lexical + structured"
    if entities.semantic_topic:
        mode = "hybrid lexical + semantic ranking"

    lines = [
        f"UI mode: {query_mode}",
        f"Retrieval mode: {mode}",
        f"Context sample values: {sample_count}",
        f"Similar NL2SQL examples: {example_count}",
        "Generate SQL, validate, execute, and repair on failure.",
    ]
    return "\n".join(lines)


def execute_query_with_retry(
    runtime: Dict[str, Any],
    user_question: str,
    max_attempts: int,
    query_mode: str,
    max_rows: int,
) -> Tuple[Optional[pd.DataFrame], str, List[Dict[str, Any]], Dict[str, Any]]:
    t0 = time.perf_counter()

    embedder = runtime["embedder"]
    generator = runtime["generator"]
    repair = runtime["repair"]
    retriever = runtime["retriever"]
    executor = runtime["executor"]

    cfg = _QUERY_MODE_CONFIG.get(query_mode, _QUERY_MODE_CONFIG["Balanced"])
    old_catalog_top_k = retriever.catalog_top_k
    old_history_top_k = retriever.history_top_k
    old_max_rows = executor.max_rows

    retriever.catalog_top_k = int(cfg["catalog_top_k"])
    retriever.history_top_k = int(cfg["history_top_k"])
    executor.max_rows = max_rows

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
    prompt += "\n\nQUERY QUALITY MODE OVERRIDE:\n" + str(cfg["prompt_hint"]) + "\n"

    attempt_history: List[Dict[str, Any]] = []
    try:
        sql = generator.generate(prompt)
    except Exception as exc:
        err = str(exc)
        if "GEMINI_API_KEY" in err:
            err = (
                "Gemini is selected but GEMINI_API_KEY is missing. "
                "Set GEMINI_API_KEY in your .env or shell, then reconnect."
            )
        attempt_history.append(
            {
                "attempt_number": 1,
                "understanding": _understanding_text(user_question, entities),
                "plan": _plan_text(
                    entities,
                    sample_count=len(context.sample_values),
                    example_count=len(context.similar_examples),
                    query_mode=query_mode,
                ),
                "sql": "",
                "validation": {"warnings": []},
                "error": err,
                "diagnosis": "LLM generation failed before SQL was produced.",
            }
        )
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        return None, "", attempt_history, {
            "query_embedding": query_embedding,
            "attempt_count": 1,
            "execution_time_ms": elapsed_ms,
            "repaired": False,
            "error_text": err,
            "row_count": 0,
        }

    final_error: Optional[str] = None

    try:
        for attempt_number in range(1, max_attempts + 1):
            validation = {"warnings": []}
            diagnosis = None

            try:
                validate_sql(sql)
            except SQLValidationError as exc:
                err = str(exc)
                final_error = err
                diagnosis = "Validator rejected SQL before execution."
                attempt_history.append(
                    {
                        "attempt_number": attempt_number,
                        "understanding": _understanding_text(user_question, entities),
                        "plan": _plan_text(
                            entities,
                            sample_count=len(context.sample_values),
                            example_count=len(context.similar_examples),
                            query_mode=query_mode,
                        ),
                        "sql": sql,
                        "validation": validation,
                        "error": err,
                        "diagnosis": diagnosis,
                    }
                )
                if attempt_number < max_attempts:
                    sql = repair.repair(sql, err)
                    continue
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                meta = {
                    "query_embedding": query_embedding,
                    "attempt_count": len(attempt_history),
                    "execution_time_ms": elapsed_ms,
                    "repaired": len(attempt_history) > 1,
                    "error_text": final_error,
                    "row_count": 0,
                }
                return None, sql, attempt_history, meta

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
                            query_mode=query_mode,
                        ),
                        "sql": sql,
                        "validation": validation,
                        "error": None,
                        "diagnosis": None,
                    }
                )
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                meta = {
                    "query_embedding": query_embedding,
                    "attempt_count": len(attempt_history),
                    "execution_time_ms": elapsed_ms,
                    "repaired": len(attempt_history) > 1,
                    "error_text": None,
                    "row_count": int(len(df)),
                }
                return df, sql, attempt_history, meta
            except Exception as exc:
                err = str(exc)
                final_error = err
                diagnosis = "Execution failed; attempting SQL correction based on DB error."
                attempt_history.append(
                    {
                        "attempt_number": attempt_number,
                        "understanding": _understanding_text(user_question, entities),
                        "plan": _plan_text(
                            entities,
                            sample_count=len(context.sample_values),
                            example_count=len(context.similar_examples),
                            query_mode=query_mode,
                        ),
                        "sql": sql,
                        "validation": validation,
                        "error": err,
                        "diagnosis": diagnosis,
                    }
                )
                if attempt_number < max_attempts:
                    sql = repair.repair(sql, err)
                    continue

        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        meta = {
            "query_embedding": query_embedding,
            "attempt_count": len(attempt_history),
            "execution_time_ms": elapsed_ms,
            "repaired": len(attempt_history) > 1,
            "error_text": final_error,
            "row_count": 0,
        }
        return None, sql, attempt_history, meta
    finally:
        retriever.catalog_top_k = old_catalog_top_k
        retriever.history_top_k = old_history_top_k
        executor.max_rows = old_max_rows


def run_sql_workbench(
    runtime: Dict[str, Any],
    sql_text: str,
    query_embedding,
    max_rows: int,
) -> Tuple[Optional[pd.DataFrame], Optional[str], int]:
    t0 = time.perf_counter()
    executor = runtime["executor"]
    old_max_rows = executor.max_rows
    executor.max_rows = max_rows

    try:
        validate_sql(sql_text)
        rows = executor.execute(
            sql_text,
            query_embedding=query_embedding if ":query_embedding" in sql_text else None,
        )
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        return pd.DataFrame(rows), None, elapsed_ms
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        return None, str(exc), elapsed_ms
    finally:
        executor.max_rows = old_max_rows


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

    lines.append("\nSQL was generated and validated through the NL2SQL pipeline.")
    lines.append(f"\nFinal SQL length: {len(final_sql)} chars")
    return "\n".join(lines)


def _detect_query_history_embedding_kind(engine) -> str:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT data_type, udt_name
                    FROM information_schema.columns
                    WHERE table_schema = 'afm'
                      AND table_name = 'query_history'
                      AND column_name = 'embedding'
                    """
                )
            ).fetchone()
        if not row:
            return "unknown"
        data_type = (row[0] or "").lower()
        udt_name = (row[1] or "").lower()
        if udt_name == "vector":
            return "vector"
        if data_type == "bytea":
            return "bytea"
    except Exception:
        return "unknown"
    return "unknown"


def _vec_to_pg_literal(vec) -> str:
    import numpy as np

    arr = np.asarray(vec, dtype=np.float32).reshape(-1)
    return "[" + ",".join(f"{v:.6f}" for v in arr) + "]"


def _save_query_history_record(
    runtime: Dict[str, Any],
    settings: Settings,
    question: str,
    sql_text: str,
    success: bool,
    attempt_count: int,
    latency_ms: int,
    row_count: int,
    repaired: bool,
    query_mode: str,
    max_rows: int,
    error_text: Optional[str],
    edited_sql: bool,
    query_embedding,
) -> Optional[str]:
    history_id = str(uuid.uuid4())
    engine = runtime["engine"]

    emb_kind = runtime.get("query_history_embedding_kind")
    if not emb_kind:
        emb_kind = _detect_query_history_embedding_kind(engine)
        runtime["query_history_embedding_kind"] = emb_kind

    emb_value = None
    if query_embedding is not None:
        if emb_kind == "vector":
            emb_value = _vec_to_pg_literal(query_embedding)
        elif emb_kind == "bytea":
            emb_value = EmbeddingBackend.vec_to_bytes(query_embedding)

    llm_backend_name = type(runtime["generator"].backend).__name__

    try:
        with engine.begin() as conn:
            if emb_kind == "vector":
                conn.execute(
                    text(
                        """
                        INSERT INTO afm.query_history (
                          id, question, generated_sql, execution_success,
                          app_source, attempt_count, execution_time_ms, row_count,
                          repaired, query_mode, max_rows,
                          llm_backend, llm_model, edited_sql, error_text,
                          embedding
                        )
                        VALUES (
                          CAST(:id AS uuid), :q, :sql, :ok,
                          :src, :attempt_count, :lat_ms, :row_count,
                          :repaired, :query_mode, :max_rows,
                          :llm_backend, :llm_model, :edited_sql, :error_text,
                          CAST(:emb AS vector)
                        )
                        """
                    ),
                    {
                        "id": history_id,
                        "q": question,
                        "sql": sql_text,
                        "ok": success,
                        "src": "streamlit",
                        "attempt_count": attempt_count,
                        "lat_ms": latency_ms,
                        "row_count": row_count,
                        "repaired": repaired,
                        "query_mode": query_mode,
                        "max_rows": max_rows,
                        "llm_backend": llm_backend_name,
                        "llm_model": settings.llm_model_name,
                        "edited_sql": edited_sql,
                        "error_text": error_text,
                        "emb": emb_value,
                    },
                )
            else:
                conn.execute(
                    text(
                        """
                        INSERT INTO afm.query_history (
                          id, question, generated_sql, execution_success,
                          app_source, attempt_count, execution_time_ms, row_count,
                          repaired, query_mode, max_rows,
                          llm_backend, llm_model, edited_sql, error_text,
                          embedding
                        )
                        VALUES (
                          CAST(:id AS uuid), :q, :sql, :ok,
                          :src, :attempt_count, :lat_ms, :row_count,
                          :repaired, :query_mode, :max_rows,
                          :llm_backend, :llm_model, :edited_sql, :error_text,
                          :emb
                        )
                        """
                    ),
                    {
                        "id": history_id,
                        "q": question,
                        "sql": sql_text,
                        "ok": success,
                        "src": "streamlit",
                        "attempt_count": attempt_count,
                        "lat_ms": latency_ms,
                        "row_count": row_count,
                        "repaired": repaired,
                        "query_mode": query_mode,
                        "max_rows": max_rows,
                        "llm_backend": llm_backend_name,
                        "llm_model": settings.llm_model_name,
                        "edited_sql": edited_sql,
                        "error_text": error_text,
                        "emb": emb_value,
                    },
                )
        return history_id
    except Exception:
        return None


def _submit_feedback(runtime: Dict[str, Any], history_id: str, score: int, note: str) -> bool:
    engine = runtime["engine"]
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE afm.query_history
                    SET user_feedback = :score,
                        feedback_note = :note,
                        feedback_at = now()
                    WHERE id = CAST(:id AS uuid)
                    """
                ),
                {"score": score, "note": note if note else None, "id": history_id},
            )
        return True
    except Exception:
        return False


def _add_saved_prompt(prompt_text: str, favorite: bool) -> None:
    prompt = prompt_text.strip()
    if not prompt:
        return

    for p in st.session_state.saved_prompts:
        if p["text"].strip().lower() == prompt.lower():
            if favorite:
                p["favorite"] = True
            return

    st.session_state.saved_prompts.append(
        {
            "id": str(uuid.uuid4()),
            "text": prompt,
            "favorite": bool(favorite),
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )


def _toggle_saved_favorite(prompt_id: str) -> None:
    for p in st.session_state.saved_prompts:
        if p["id"] == prompt_id:
            p["favorite"] = not p["favorite"]
            return


def _delete_saved_prompt(prompt_id: str) -> None:
    st.session_state.saved_prompts = [p for p in st.session_state.saved_prompts if p["id"] != prompt_id]


def _sorted_saved_prompts() -> List[Dict[str, Any]]:
    return sorted(
        st.session_state.saved_prompts,
        key=lambda x: (not bool(x.get("favorite")), x.get("created_at", "")),
    )


def _render_sidebar() -> None:
    st.markdown("## Settings")

    selected_theme = st.radio(
        "Theme",
        options=["Dark", "Light"],
        index=0 if st.session_state.theme_mode == "Dark" else 1,
        horizontal=True,
    )
    if selected_theme != st.session_state.theme_mode:
        st.session_state.theme_mode = selected_theme
        st.rerun()

    st.markdown("### Query policy")
    st.session_state.query_mode = st.selectbox(
        "Mode",
        options=["Strict", "Balanced", "Broad semantic"],
        index=["Strict", "Balanced", "Broad semantic"].index(st.session_state.query_mode),
        help="Strict is precise, Broad semantic retrieves wider contextual candidates.",
    )
    st.session_state.max_rows = st.slider(
        "Max rows",
        min_value=50,
        max_value=5000,
        value=int(st.session_state.max_rows),
        step=50,
    )
    st.session_state.max_attempts = st.slider(
        "Repair attempts",
        min_value=1,
        max_value=5,
        value=int(st.session_state.max_attempts),
    )
    st.session_state.show_reasoning = st.checkbox("Show reasoning traces", value=st.session_state.show_reasoning)

    c1, c2 = st.columns(2)
    with c1:
        reconnect_clicked = st.button("Reconnect", width="stretch", type="primary")
    with c2:
        clear_clicked = st.button("Clear chat", width="stretch")

    if reconnect_clicked:
        _initialize_runtime_if_needed(force=True)
        st.rerun()

    if clear_clicked:
        st.session_state.query_history = []
        st.session_state.chat_messages = []
        st.session_state.feedback_message = None
        st.rerun()

    st.markdown("---")
    with st.expander("Health and diagnostics", expanded=False):
        check_clicked = st.button("Run health check", width="stretch")
        if check_clicked and st.session_state.runtime is not None and st.session_state.settings is not None:
            st.session_state.health_report = _run_runtime_health(
                st.session_state.runtime,
                st.session_state.settings,
                st.session_state.runtime_error,
            )

        if st.session_state.health_report:
            st.json(st.session_state.health_report)
        elif st.session_state.runtime_error:
            st.error(st.session_state.runtime_error)
        else:
            st.caption("No diagnostics yet.")

    with st.expander("Schema", expanded=False):
        if st.session_state.settings is not None:
            st.code(_get_live_schema(st.session_state.settings), language="text")

    with st.expander("Loaded settings", expanded=False):
        if st.session_state.settings is not None:
            safe_settings = asdict(st.session_state.settings)
            safe_settings["pg_dsn"] = "***hidden***"
            st.json(safe_settings)


def _render_query_results(
    question: str,
    result_df: Optional[pd.DataFrame],
    final_sql: str,
    attempt_history: List[Dict[str, Any]],
    meta: Dict[str, Any],
    history_id: Optional[str],
) -> None:
    if result_df is None:
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
        return

    st.success("Query executed successfully.")
    st.caption(
        f"Rows: {meta.get('row_count', len(result_df))} | Attempts: {meta.get('attempt_count', 1)} | "
        f"Latency: {meta.get('execution_time_ms', 0)} ms | Repaired: {meta.get('repaired', False)}"
    )

    tabs = st.tabs(["Answer", "Data", "SQL", "Reasoning", "Feedback"])

    with tabs[0]:
        st.markdown(format_natural_language_response(question, result_df, final_sql))

    with tabs[1]:
        safe_df = _to_display_df(result_df)
        st.dataframe(safe_df, width="stretch")

        dc1, dc2 = st.columns(2)
        with dc1:
            st.download_button(
                label="Download CSV",
                data=_to_csv_bytes(safe_df),
                file_name="nl2sql_results.csv",
                mime="text/csv",
                width="stretch",
            )
        with dc2:
            st.download_button(
                label="Download XLSX",
                data=_to_xlsx_bytes(safe_df),
                file_name="nl2sql_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width="stretch",
            )

    with tabs[2]:
        st.code(final_sql, language="sql")

    with tabs[3]:
        if not st.session_state.show_reasoning:
            st.info("Enable 'Show reasoning' in the sidebar to inspect attempts.")
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

    with tabs[4]:
        if not history_id:
            st.warning("History row was not saved, feedback cannot be persisted for this run.")
        else:
            score_key = f"feedback_score_{history_id}"
            note_key = f"feedback_note_{history_id}"
            if score_key not in st.session_state:
                st.session_state[score_key] = "None"
            if note_key not in st.session_state:
                st.session_state[note_key] = ""

            st.radio(
                "How useful was this answer?",
                ["None", "Useful", "Not useful"],
                key=score_key,
                horizontal=True,
            )
            st.text_area("Correction note (optional)", key=note_key, height=100)
            if st.button("Submit feedback", key=f"feedback_submit_{history_id}", width="stretch"):
                score_raw = st.session_state[score_key]
                score_val = 0
                if score_raw == "Useful":
                    score_val = 1
                elif score_raw == "Not useful":
                    score_val = -1

                note_text = str(st.session_state[note_key]).strip()
                if score_val == 0 and not note_text:
                    st.warning("Select a score or provide a note.")
                else:
                    ok = _submit_feedback(st.session_state.runtime, history_id, score_val, note_text)
                    if ok:
                        st.success("Feedback saved.")
                        for item in st.session_state.query_history:
                            if item.get("history_id") == history_id:
                                item["feedback"] = score_val
                                item["feedback_note"] = note_text
                    else:
                        st.error("Failed to save feedback.")


def _render_saved_prompts_tab() -> None:
    st.markdown("### Saved prompts and favorites")

    new_prompt = st.text_area(
        "Add new prompt",
        placeholder="Type a business question to save...",
        key="saved_prompt_input",
        height=90,
    )
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Save Prompt", width="stretch"):
            _add_saved_prompt(new_prompt, favorite=False)
            st.success("Prompt saved.")
    with c2:
        if st.button("Save as Favorite", width="stretch"):
            _add_saved_prompt(new_prompt, favorite=True)
            st.success("Favorite saved.")

    st.markdown("---")
    prompts = _sorted_saved_prompts()
    if not prompts:
        st.info("No saved prompts yet.")
        return

    for p in prompts:
        star = "[fav]" if p.get("favorite") else "[ ]"
        st.markdown(f"**{star}** {p['text']}")
        cols = st.columns([1, 1, 1])
        with cols[0]:
            if st.button("Run", key=f"run_prompt_{p['id']}", width="stretch"):
                st.session_state.queued_prompt = p["text"]
                st.rerun()
        with cols[1]:
            if st.button("Toggle Fav", key=f"fav_prompt_{p['id']}", width="stretch"):
                _toggle_saved_favorite(p["id"])
                st.rerun()
        with cols[2]:
            if st.button("Delete", key=f"del_prompt_{p['id']}", width="stretch"):
                _delete_saved_prompt(p["id"])
                st.rerun()
        st.caption(f"Saved at {p.get('created_at', '')}")
        st.markdown("---")


def _append_session_audit(entry: Dict[str, Any]) -> None:
    st.session_state.query_history.append(entry)


def _render_chat_history() -> None:
    for msg in st.session_state.chat_messages:
        with st.chat_message(msg.get("role", "assistant")):
            if msg.get("role") == "user":
                st.markdown(msg.get("content", ""))
                continue

            if msg.get("error"):
                st.error(msg["error"])
            else:
                st.markdown(msg.get("content", ""))

            if msg.get("meta"):
                meta = msg["meta"]
                st.caption(
                    f"Rows: {meta.get('row_count', 0)} | Attempts: {meta.get('attempt_count', 1)} | "
                    f"Latency: {meta.get('execution_time_ms', 0)} ms | Repaired: {meta.get('repaired', False)}"
                )

            if msg.get("sql"):
                with st.expander("Generated SQL", expanded=False):
                    st.code(msg["sql"], language="sql")

            rows_preview = msg.get("rows_preview")
            if rows_preview:
                with st.expander("Result preview", expanded=False):
                    st.dataframe(pd.DataFrame(rows_preview), width="stretch")


def main() -> None:
    _style_page()
    _init_session_state()
    _initialize_runtime_if_needed()

    with st.sidebar:
        _render_sidebar()

    if st.session_state.runtime is None or st.session_state.settings is None:
        st.error("Runtime is not ready.")
        if st.session_state.runtime_error:
            st.code(st.session_state.runtime_error, language="text")
        st.info("Check .env values and click Reconnect from the sidebar.")
        st.stop()

    st.markdown("### Financial Assistant")
    st.caption("Chat with your AFM NL2SQL assistant. Ask naturally and get database-backed answers.")

    _render_chat_history()

    user_prompt = st.chat_input("Ask about transactions, loans, dates, and amounts...")
    queued_prompt = str(st.session_state.get("queued_prompt", "")).strip()
    prompt_to_run = (user_prompt or "").strip() or queued_prompt

    if prompt_to_run:
        if queued_prompt and not user_prompt:
            st.session_state.queued_prompt = ""

        st.session_state.chat_messages.append({"role": "user", "content": prompt_to_run})

        with st.chat_message("assistant"):
            with st.spinner("Thinking and generating SQL..."):
                result_df, final_sql, attempt_history, meta = execute_query_with_retry(
                    st.session_state.runtime,
                    prompt_to_run,
                    max_attempts=st.session_state.max_attempts,
                    query_mode=st.session_state.query_mode,
                    max_rows=st.session_state.max_rows,
                )

            st.session_state.last_question = prompt_to_run
            st.session_state.last_result_df = result_df
            st.session_state.last_result_sql = final_sql
            st.session_state.last_attempt_history = attempt_history
            st.session_state.last_result_meta = meta
            st.session_state.last_generated_sql = final_sql
            st.session_state.sql_editor_text = final_sql
            st.session_state.last_query_embedding = meta.get("query_embedding")

            history_id = _save_query_history_record(
                st.session_state.runtime,
                st.session_state.settings,
                question=prompt_to_run,
                sql_text=final_sql,
                success=result_df is not None,
                attempt_count=int(meta.get("attempt_count", 1)),
                latency_ms=int(meta.get("execution_time_ms", 0)),
                row_count=int(meta.get("row_count", 0)),
                repaired=bool(meta.get("repaired", False)),
                query_mode=st.session_state.query_mode,
                max_rows=int(st.session_state.max_rows),
                error_text=meta.get("error_text"),
                edited_sql=False,
                query_embedding=meta.get("query_embedding"),
            )
            st.session_state.last_history_id = history_id

            _append_session_audit(
                {
                    "history_id": history_id,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "question": prompt_to_run,
                    "sql": final_sql,
                    "rows": int(meta.get("row_count", 0)),
                    "attempts": int(meta.get("attempt_count", 1)),
                    "latency_ms": int(meta.get("execution_time_ms", 0)),
                    "repaired": bool(meta.get("repaired", False)),
                    "query_mode": st.session_state.query_mode,
                    "max_rows": int(st.session_state.max_rows),
                    "edited_sql": False,
                    "success": result_df is not None,
                    "feedback": None,
                    "feedback_note": None,
                }
            )

            if result_df is None:
                error_text = meta.get("error_text") or "Query failed after all attempts."
                st.error(error_text)
                if st.session_state.show_reasoning and attempt_history:
                    with st.expander("Attempt details", expanded=False):
                        for attempt in attempt_history:
                            st.error(f"Attempt {attempt['attempt_number']}")
                            if attempt.get("sql"):
                                st.code(attempt["sql"], language="sql")
                            if attempt.get("error"):
                                st.error(f"Error: {attempt['error']}")
                            if attempt.get("diagnosis"):
                                st.warning(attempt["diagnosis"])
                st.session_state.chat_messages.append(
                    {
                        "role": "assistant",
                        "content": "I could not complete that request.",
                        "error": error_text,
                        "meta": meta,
                        "sql": final_sql,
                        "rows_preview": [],
                    }
                )
            else:
                answer_text = format_natural_language_response(prompt_to_run, result_df, final_sql)
                st.markdown(answer_text)
                st.caption(
                    f"Rows: {meta.get('row_count', len(result_df))} | Attempts: {meta.get('attempt_count', 1)} | "
                    f"Latency: {meta.get('execution_time_ms', 0)} ms | Repaired: {meta.get('repaired', False)}"
                )
                with st.expander("Generated SQL", expanded=False):
                    st.code(final_sql, language="sql")
                with st.expander("Result preview", expanded=False):
                    st.dataframe(_to_display_df(result_df).head(200), width="stretch")

                preview_rows = _to_display_df(result_df).head(40).to_dict("records")
                st.session_state.chat_messages.append(
                    {
                        "role": "assistant",
                        "content": answer_text,
                        "error": None,
                        "meta": meta,
                        "sql": final_sql,
                        "rows_preview": preview_rows,
                    }
                )

        st.rerun()

    with st.expander("Prompt library", expanded=False):
        _render_saved_prompts_tab()

    with st.expander("Run log", expanded=False):
        if not st.session_state.query_history:
            st.info("No entries yet.")
        else:
            audit_df = _to_display_df(pd.DataFrame(st.session_state.query_history))
            st.dataframe(audit_df, width="stretch")
            st.download_button(
                label="Download Audit CSV",
                data=_to_csv_bytes(audit_df),
                file_name="nl2sql_audit_log.csv",
                mime="text/csv",
                width="stretch",
            )


if __name__ == "__main__":
    main()
