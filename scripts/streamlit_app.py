"""
scripts/streamlit_app.py
AFM Financial Intelligence Platform — полный дашборд
Вкладки: Чат | Дашборд | Загрузка выписок
"""
from __future__ import annotations
import asyncio
import datetime
import os
import sys
import time
import tempfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import settings
from app.database import engine
from app.db.schema import ensure_schema
from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
from app.nl2sql.query_service import QueryService
from app.nl2sql.sql_generator import build_llm_backend

# ─── page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="AFM — Финансовая аналитика",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
[data-testid="stChatInput"] { border-radius: 30px !important; }
[data-testid="stChatInput"] > div { border-radius: 30px !important; }
[data-testid="stChatInputTextArea"] { border-radius: 30px !important; }
[data-testid="stChatInputSubmitButton"] { border-radius: 50% !important; }
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
.metric-card {
    background: #f8f9fa;
    border-radius: 12px;
    padding: 16px 20px;
    border: 1px solid #e9ecef;
}
</style>
""", unsafe_allow_html=True)

# ─── services ─────────────────────────────────────────────────────────────────
@st.cache_resource(ttl="1h")
def get_query_service():
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
    intent_backend = None
    if settings.AFM_INTENT_LLM_MODEL:
        intent_backend = build_llm_backend(
            model_name=settings.AFM_INTENT_LLM_MODEL,
            base_url=settings.llm_base_url,
            timeout_s=settings.llm_timeout_s,
        )
    return QueryService.build(
        engine, embedder, llm_backend,
        intent_backend=intent_backend,
        max_new_tokens=settings.llm_max_new_tokens,
    )

executor = ThreadPoolExecutor(max_workers=5)
MIN_TIME_BETWEEN_REQUESTS = datetime.timedelta(seconds=3)

# ─── 19 категорий с цветами ───────────────────────────────────────────────────
CATEGORIES = {
    "P2P_ПЕРЕВОД":          ("P2P перевод",                   "#4A90D9"),
    "ПОКУПКА_В_МАГАЗИНЕ":   ("Покупка в магазине",             "#27AE60"),
    "ВНУТРЕННЯЯ_ОПЕРАЦИЯ":  ("Внутренние операции",            "#8E44AD"),
    "СНЯТИЕ_НАЛИЧНЫХ":      ("Снятие наличных",                "#E67E22"),
    "ПОГАШЕНИЕ_КРЕДИТА":    ("Погашение кредита",              "#E74C3C"),
    "ГЕМБЛИНГ":             ("Гемблинг",                       "#C0392B"),
    "ОБЯЗАТЕЛЬНЫЙ_ПЛАТЕЖ":  ("Обязательные платежи",           "#7F8C8D"),
    "ГОСВЫПЛАТА":           ("Госвыплата",                     "#2ECC71"),
    "ЗАРПЛАТА":             ("Зарплата",                       "#3498DB"),
    "ПОПОЛНЕНИЕ_СЧЕТА":     ("Пополнение счёта",               "#1ABC9C"),
    "РАСЧЕТ_ПО_ДОГОВОРУ":   ("Расчёты по договору",            "#9B59B6"),
    "ОПЛАТА_СЧЕТ_ФАКТУРЫ":  ("Оплата по счёт-фактуре",         "#6C3483"),
    "ПЛАТЕЖ_НА_КАРТУ":      ("Платёж на карту",                "#2980B9"),
    "ВАЛЮТНАЯ_ОПЕРАЦИЯ":    ("Валютная операция",              "#F39C12"),
    "ВЫДАЧА_ЗАЙМА":         ("Выдача займа",                   "#E74C3C"),
    "АЛИМЕНТЫ":             ("Алименты",                       "#117A65"),
    "ЦЕННЫЕ_БУМАГИ":        ("Ценные бумаги",                  "#1A5276"),
    "ВОЗВРАТ_СРЕДСТВ":      ("Возврат средств",                "#148F77"),
    "ПРОЧЕЕ":               ("Прочее",                         "#95A5A6"),
}

CAT_SUGGESTIONS = [
    "Покажи все транзакции по категории Зарплата",
    "Сколько всего было снятий наличных?",
    "Топ 10 по P2P переводам",
    "Баланс по категориям за 2024 год",
    "Кто чаще всего делает гемблинг платежи?",
    "Покажи все выдачи займов",
    "Сумма обязательных платежей по месяцам",
    "Транзакции на проверку (needs_review = true)",
]

# ─── helpers ──────────────────────────────────────────────────────────────────
def _run_query(question: str):
    svc = get_query_service()
    svc.executor.max_rows = st.session_state.get("max_rows", 100)
    svc.repair.max_attempts = st.session_state.get("retry_attempts", 2)
    return asyncio.run(svc.run(question))


def _stream(text: str):
    for i in range(0, len(text), 40):
        yield text[i:i+40]


def _run_ingestion(file_path: str, source_bank: str):
    from app.ingestion.pipeline import IngestionPipeline
    with IngestionPipeline() as pipeline:
        return pipeline.ingest_file(file_path, source_bank=source_bank)


def _load_category_stats() -> pd.DataFrame:
    """Load category breakdown from transactions_nl_view."""
    from sqlalchemy import text
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT
                    transaction_category,
                    transaction_category_ru,
                    category_group,
                    COUNT(*) AS tx_count,
                    COALESCE(SUM(amount_kzt),0) AS total_kzt,
                    COALESCE(SUM(CASE WHEN direction='credit' THEN amount_kzt ELSE 0 END),0) AS credit_kzt,
                    COALESCE(SUM(CASE WHEN direction='debit'  THEN amount_kzt ELSE 0 END),0) AS debit_kzt,
                    ROUND(AVG(category_confidence)::numeric, 3) AS avg_confidence,
                    COUNT(*) FILTER (WHERE category_source = 'rule') AS rule_count,
                    COUNT(*) FILTER (WHERE category_source = 'embedding') AS emb_count,
                    COUNT(*) FILTER (WHERE needs_review = TRUE) AS review_count
                FROM afm.transactions_nl_view
                WHERE transaction_category IS NOT NULL
                GROUP BY transaction_category, transaction_category_ru, category_group
                ORDER BY tx_count DESC
            """)).fetchall()
        cols = ["Код","Категория","Группа","Кол-во","Оборот KZT","Приход","Расход","Уверенность","Rule","Emb","На проверке"]
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        return pd.DataFrame(columns=["Код","Категория","Группа","Кол-во","Оборот KZT","Приход","Расход","Уверенность","Rule","Emb","На проверке"])


def _load_summary() -> dict:
    from sqlalchemy import text
    try:
        with engine.connect() as conn:
            r = conn.execute(text("""
                SELECT
                    COUNT(*) AS total,
                    COALESCE(SUM(amount_kzt),0) AS turnover,
                    COUNT(*) FILTER (WHERE needs_review = TRUE) AS review,
                    COUNT(DISTINCT transaction_category) AS cats,
                    COUNT(*) FILTER (WHERE category_source = 'rule') AS rule_count,
                    COUNT(*) FILTER (WHERE category_source = 'embedding') AS emb_count
                FROM afm.transactions_nl_view
            """)).fetchone()
        return {"total": r[0], "turnover": r[1], "review": r[2], "cats": r[3], "rule": r[4], "emb": r[5]}
    except Exception:
        return {"total": 0, "turnover": 0, "review": 0, "cats": 0, "rule": 0, "emb": 0}


# ─── dialogs ──────────────────────────────────────────────────────────────────
@st.dialog("О системе")
def show_about():
    st.markdown("""
    **AFM — Платформа финансовой разведки**

    Система анализирует банковские выписки Kaspi и Halyk и автоматически классифицирует
    каждую транзакцию в одну из **19 бизнес-категорий**.

    **Возможности:**
    - 🤖 **NL2SQL чат** — задавай вопросы на русском, получай SQL и результаты
    - 📊 **Дашборд** — диаграммы по категориям, статистика классификации
    - 📁 **Загрузка выписок** — Kaspi и Halyk через адаптеры с автоматической классификацией

    **Категории транзакций:**
    """)
    for code, (name, color) in CATEGORIES.items():
        st.markdown(f"<span style='color:{color}'>●</span> **{name}** `{code}`", unsafe_allow_html=True)


@st.dialog("Настройки")
def show_settings():
    st.session_state.max_rows = st.slider("Максимум строк", 10, 1000, st.session_state.get("max_rows", 100), 10)
    st.session_state.retry_attempts = st.slider("Попыток исправить SQL", 0, 5, st.session_state.get("retry_attempts", 2))
    if st.button("Сохранить", use_container_width=True):
        st.session_state._show_settings = False
        st.rerun()


def show_feedback(msg_idx: int):
    st.write("")
    with st.popover("Оценить ответ"):
        with st.form(key=f"fb-{msg_idx}", border=False):
            rating = st.feedback(options="stars")
            details = st.text_area("Комментарий (необязательно)")
            if st.form_submit_button("Отправить"):
                history_id = st.session_state.messages[msg_idx].get("history_id")
                if history_id:
                    try:
                        from sqlalchemy import text
                        with engine.begin() as conn:
                            conn.execute(text("""
                                UPDATE afm.query_history
                                SET user_feedback=:r, feedback_note=:d, feedback_at=now()
                                WHERE id=CAST(:id AS uuid)
                            """), {"r": (rating+1) if rating is not None else 0, "d": details, "id": history_id})
                        st.success("Спасибо!")
                    except Exception as e:
                        st.error(str(e))


# ─── main layout ──────────────────────────────────────────────────────────────
if st.session_state.get("_show_settings"):
    show_settings()

tab_chat, tab_dash, tab_upload = st.tabs(["💬 Чат", "📊 Дашборд", "📁 Загрузка выписок"])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — CHAT
# ═══════════════════════════════════════════════════════════════════════════════
with tab_chat:
    hdr = st.container(horizontal=True, vertical_alignment="bottom")
    with hdr:
        st.title("🔍 AFM Аналитика", anchor=False, width="stretch")
        if st.button("Очистить", icon=":material/refresh:"):
            st.session_state.messages = []
            st.session_state.initial_question = None
            st.session_state.selected_suggestion = None
            st.rerun()
        if st.button("О системе", type="tertiary"):
            show_about()
        if st.button("⚙️ Настройки", type="tertiary"):
            st.session_state._show_settings = True
            st.rerun()

    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "prev_ts" not in st.session_state:
        st.session_state.prev_ts = datetime.datetime.fromtimestamp(0)

    # First screen — no history yet
    if not st.session_state.messages and "initial_question" not in st.session_state:
        st.chat_input("Задай вопрос о транзакциях...", key="initial_question")
        st.pills(
            label="Примеры",
            label_visibility="collapsed",
            options=CAT_SUGGESTIONS,
            key="selected_suggestion",
        )
        st.stop()

    user_message = st.chat_input("Уточни запрос...")
    if not user_message:
        if st.session_state.get("initial_question"):
            user_message = st.session_state.initial_question
        elif st.session_state.get("selected_suggestion"):
            user_message = st.session_state.selected_suggestion

    # Render history
    for i, msg in enumerate(st.session_state.messages):
        with st.chat_message(msg["role"]):
            if msg["role"] == "assistant":
                st.container()
            st.markdown(msg["content"])
            if msg.get("sql_block"):
                with st.expander(msg.get("sql_header", "SQL")):
                    st.markdown(msg["sql_block"])
            if isinstance(msg.get("rows"), pd.DataFrame):
                st.dataframe(msg["rows"])
            if msg["role"] == "assistant":
                show_feedback(i)

    if user_message:
        user_message = user_message.replace("$", r"\$")

        with st.chat_message("user"):
            st.text(user_message)

        with st.chat_message("assistant"):
            now = datetime.datetime.now()
            diff = now - st.session_state.prev_ts
            st.session_state.prev_ts = now
            if diff < MIN_TIME_BETWEEN_REQUESTS:
                time.sleep(diff.total_seconds())

            clean_q = user_message.replace("'", "")

            with st.spinner("Генерирую SQL..."):
                try:
                    result = _run_query(clean_q)
                    if result.error:
                        resp = f"**Ошибка:**\n```\n{result.error}\n```"
                        sql_block = None
                        sql_header = None
                    else:
                        repaired = " *(исправлен)*" if result.repaired else ""
                        sql_header = f"**SQL ({result.execution_time_s:.2f}s){repaired}**"
                        sql_block = f"```sql\n{result.sql}\n```" if result.sql else None
                        n = len(result.rows)
                        if result.ai_summary and not result.rows:
                            resp = result.ai_summary
                        elif result.rows:
                            resp = f"{result.ai_summary or 'Результаты:'}\n\n**{n} строк**"
                        else:
                            resp = "Данные не найдены."
                except Exception as e:
                    resp = f"**Ошибка pipeline:**\n```\n{e}\n```"
                    sql_block = None
                    sql_header = None
                    result = None

            with st.container():
                response = st.write_stream(_stream(resp))
                if sql_block:
                    with st.expander(sql_header):
                        st.markdown(sql_block)

                df_rows = None
                if result and not result.error and result.rows:
                    df_rows = pd.DataFrame(result.rows)
                    for col in df_rows.columns:
                        if df_rows[col].dtype == "object":
                            df_rows[col] = df_rows[col].astype(str)
                    st.dataframe(df_rows)

                st.session_state.messages.append({"role": "user", "content": user_message})
                msg_dict = {
                    "role": "assistant",
                    "content": response,
                    "sql_header": sql_header if "sql_header" in locals() else None,
                    "sql_block": sql_block if "sql_block" in locals() else None,
                    "rows": df_rows,
                }
                if result and result.history_id:
                    msg_dict["history_id"] = result.history_id
                st.session_state.messages.append(msg_dict)
                show_feedback(len(st.session_state.messages) - 1)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════
with tab_dash:
    st.subheader("📊 Дашборд по категориям")

    if st.button("🔄 Обновить данные", key="refresh_dash"):
        st.cache_data.clear()

    # KPI summary
    summary = _load_summary()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Всего транзакций", f"{summary['total']:,}")
    c2.metric("Оборот KZT", f"{summary['turnover']/1_000_000:.1f}M")
    c3.metric("На проверке", f"{summary['review']:,}")
    c4.metric("Правила (rule)", f"{summary['rule']:,}")
    c5.metric("Embedding", f"{summary['emb']:,}")

    st.divider()

    df = _load_category_stats()

    if df.empty:
        st.info("Нет данных. Загрузите выписки во вкладке «Загрузка выписок».")
    else:
        # Pie chart — distribution by count
        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("**Распределение по количеству транзакций**")
            try:
                import plotly.express as px
                fig = px.pie(
                    df,
                    names="Категория",
                    values="Кол-во",
                    color="Категория",
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Set3,
                )
                fig.update_traces(textposition="inside", textinfo="percent+label")
                fig.update_layout(
                    showlegend=False,
                    margin=dict(t=10, b=10, l=10, r=10),
                    height=380,
                )
                st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                # fallback without plotly
                st.bar_chart(df.set_index("Категория")["Кол-во"])

        with col_right:
            st.markdown("**Оборот по категориям (KZT)**")
            try:
                import plotly.express as px
                df_sorted = df.sort_values("Оборот KZT", ascending=True).tail(15)
                fig2 = px.bar(
                    df_sorted,
                    x="Оборот KZT",
                    y="Категория",
                    orientation="h",
                    color="Группа",
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                )
                fig2.update_layout(
                    margin=dict(t=10, b=10, l=10, r=10),
                    height=380,
                    showlegend=True,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig2, use_container_width=True)
            except ImportError:
                st.bar_chart(df.set_index("Категория")["Оборот KZT"])

        st.divider()

        # Source reliability chart
        st.markdown("**Источник классификации по категориям**")
        try:
            import plotly.graph_objects as go
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(name="Правила (rule)", x=df["Категория"], y=df["Rule"], marker_color="#3498DB"))
            fig3.add_trace(go.Bar(name="Embedding", x=df["Категория"], y=df["Emb"], marker_color="#E67E22"))
            fig3.add_trace(go.Bar(name="На проверке", x=df["Категория"], y=df["На проверке"], marker_color="#E74C3C"))
            fig3.update_layout(
                barmode="stack",
                height=320,
                margin=dict(t=10, b=80, l=10, r=10),
                xaxis_tickangle=-35,
                legend=dict(orientation="h"),
            )
            st.plotly_chart(fig3, use_container_width=True)
        except ImportError:
            pass

        st.divider()

        # Full table
        st.markdown("**Детальная таблица по категориям**")
        display_df = df[["Категория","Группа","Кол-во","Оборот KZT","Приход","Расход","Уверенность","Rule","Emb","На проверке"]].copy()
        for col in ["Оборот KZT","Приход","Расход"]:
            display_df[col] = display_df[col].apply(lambda x: f"{x:,.0f}")
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Quick filter by category → send to chat
        st.divider()
        st.markdown("**Быстрый запрос по категории**")
        cats = df["Категория"].tolist()
        sel = st.selectbox("Выбери категорию:", cats, key="quick_cat")
        if st.button("Открыть в чате →", key="go_chat"):
            question = f"Покажи все транзакции по категории {sel}, топ 50 по сумме"
            st.session_state.initial_question = question
            st.session_state.messages = []
            st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — UPLOAD
# ═══════════════════════════════════════════════════════════════════════════════
with tab_upload:
    st.subheader("📁 Загрузка банковских выписок")

    st.info("""
    **Как это работает:**
    1. Выбери банк (Kaspi или Halyk)
    2. Загрузи файл выписки (.xlsx или .xls)
    3. Система автоматически:
       - Парсит через специализированный адаптер банка
       - Классифицирует каждую транзакцию (19 категорий)
       - Сохраняет в базу с дедупликацией
    """)

    col_bank, col_file = st.columns([1, 2])

    with col_bank:
        bank = st.radio(
            "**Банк:**",
            options=["kaspi", "halyk"],
            format_func=lambda x: "🔵 Kaspi Bank" if x == "kaspi" else "🟢 Halyk Bank",
            key="upload_bank",
        )
        st.markdown("---")
        st.markdown(f"**Выбран:** {'Kaspi Bank' if bank == 'kaspi' else 'Halyk Bank'}")
        st.markdown(f"**Формат:** .xlsx / .xls")
        st.markdown(f"**Адаптер:** `{bank}_adapter.py`")

    with col_file:
        uploaded = st.file_uploader(
            f"Загрузи выписку {bank.upper()}",
            type=["xlsx", "xls"],
            key="file_uploader",
            help="Поддерживается Excel-выписка из интернет-банка",
        )

        if uploaded:
            st.success(f"✅ Файл выбран: **{uploaded.name}** ({uploaded.size // 1024} KB)")

            if st.button("🚀 Загрузить и обработать", type="primary", use_container_width=True, key="do_upload"):
                suffix = ".xlsx" if uploaded.name.endswith(".xlsx") else ".xls"
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    tmp.write(uploaded.read())
                    tmp_path = tmp.name

                with st.spinner(f"Обрабатываем через {bank} адаптер..."):
                    try:
                        result = _run_ingestion(tmp_path, source_bank=bank)
                    except Exception as e:
                        st.error(f"❌ Ошибка: {e}")
                        result = None
                    finally:
                        try:
                            os.unlink(tmp_path)
                        except OSError:
                            pass

                if result:
                    st.success(f"✅ Готово!")
                    res_col1, res_col2, res_col3, res_col4 = st.columns(4)
                    res_col1.metric("Строк обработано", result.get("core_rows", 0))
                    res_col2.metric("Выписок", result.get("statements", 0))
                    res_col3.metric("Классифицировано", result.get("categories_assigned", 0))
                    res_col4.metric("Банк", result.get("bank", bank).upper())

                    if result.get("core_rows", 0) == 0:
                        st.warning("⚠️ Строк не найдено. Проверь формат файла.")
                    else:
                        st.info("💡 Перейди в вкладку **Дашборд** для просмотра статистики по категориям.")

    st.divider()

    # Supported formats reference
    with st.expander("📋 Поддерживаемые форматы файлов"):
        st.markdown("""
        **Kaspi Bank:**
        - Выписка из Kaspi Business (17 колонок)
        - Включает: плательщик, получатель, ИИН/БИН, назначение, КНП

        **Halyk Bank:**
        - Стандартная выписка Halyk (входящий/исходящий формат)
        - Включает: ВХ/ИСХ, назначение платежа, суммы

        **Категории назначаются автоматически:**
        """)
        for code, (name, color) in list(CATEGORIES.items())[:10]:
            st.markdown(f"<span style='color:{color}'>●</span> {name}", unsafe_allow_html=True)
        st.markdown("... и ещё 9 категорий")
