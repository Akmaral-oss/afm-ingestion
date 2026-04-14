"""
Analytics endpoints:
  GET /api/v1/analytics/time-series
  GET /api/v1/analytics/summary
  GET /api/v1/analytics/top-expenses
  GET /api/v1/analytics/top-counterparties
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, and_, or_, case, literal, literal_column, cast, DateTime, Date, union_all
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import Transaction
from ..project_context import ProjectContext, get_current_project_context
from ..utils.text_utils import clean_optional_text
from app.exceptions import MissingIdentityFieldsException
from ..schemas import (
    TimeSeriesResponse,
    TimeSeriesPoint,
    TimeSeriesTransactionsResponse,
    AnalyticsSummaryResponse,
    PeriodRange,
    TopExpensesResponse,
    TopExpenseItem,
    CounterpartyOut,
    TopCounterpartiesResponse,
    TopCounterpartyItem,
    CounterpartySearchResponse,
    CounterpartySearchItem,
    CashTransactionsResponse,
    CashTransactionItem,
    CounterpartyTransactionsResponse,
    EdgeTransactionsResponse,
    CounterpartyGraphResponse,
    CounterpartyGraphNode,
    CounterpartyGraphEdge,
    CategorySummaryResponse,
    CategorySummaryItem,
    ComparePeriodSummary,
    ComparePeriodDelta,
    ComparePeriodMetric,
    ComparePeriodCategoryItem,
    ComparePeriodsResponse,
)

router = APIRouter(prefix="/analytics", tags=["Analytics"])

MONTHS_RU = [
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
]


def _parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%d.%m.%Y")


def _normalize_iin(value: str) -> str:
    return "".join(ch for ch in (value or "") if ch.isalnum()).upper()


def _normalize_account(value: str) -> str:
    return "".join(ch for ch in (value or "") if ch.isalnum()).upper()


def _normalize_name(value: str) -> str:
    cleaned = clean_optional_text(value)
    if not cleaned:
        return ""
    s = cleaned.strip().strip('"').strip("'").lower()
    return " ".join(s.split())

def _fix_mojibake(value: Optional[str]) -> str:
    s = (value or "").strip()
    if not s:
        return ""
    try:
        decoded = s.encode("cp1251").decode("utf-8")
        if decoded:
            s = decoded
    except Exception:
        pass
    return s.replace("С‘", "ё").replace("Рµ", "е")


def _to_mojibake(value: str) -> str:
    s = (value or "").strip()
    if not s:
        return ""
    try:
        return s.encode("utf-8").decode("cp1251")
    except Exception:
        return s


def _derived_category_expr():
    trimmed_category = func.nullif(func.trim(Transaction.transaction_category), "")
    trimmed_legacy_category = func.nullif(func.trim(Transaction.category), "")
    purpose = func.lower(func.coalesce(Transaction.purpose, ""))
    op_type = func.lower(func.coalesce(Transaction.operation_type, ""))
    direction = func.lower(func.coalesce(Transaction.direction, ""))
    return func.coalesce(
        trimmed_category,
        trimmed_legacy_category,
        case(
            (or_(purpose.like("%red.kz%"), purpose.like("%продаж%")), "Продажи Kaspi / Red"),
            (or_(purpose.like("%погаш%"), purpose.like("%кредит%")), "Погашение кредита"),
            (or_(purpose.like("%снятие%"), purpose.like("%cash%"), purpose.like("%atm%")), "Снятие наличных"),
            (or_(purpose.like("%ресайклер%"), purpose.like("%recycler%")), "Пополнение наличными"),
            (or_(purpose.like("%пополн%"), purpose.like("%взнос%"), purpose.like("%deposit%")), "Пополнение счёта"),
            (purpose.like("%рассроч%"), "Рассрочка Kaspi"),
            (purpose.like("%займ%"), "Выдача займа"),
            (and_(purpose.like("%перевод%"), purpose.like("%внутр%")), "Внутренние операции"),
            (or_(purpose.like("%перевод%"), op_type.like("%payment%")), "P2P перевод"),
            (purpose.like("%оплат%"), "Оплата услуг"),
            (direction == "credit", "Поступления"),
            (direction == "debit", "Расходы"),
            else_="Прочее",
        ),
    )


def _is_invalid_iin(value: Optional[str]) -> bool:
    iin = (value or "").strip()
    return not iin or iin in {"0", "000000000000"}


def _is_unknown_name(value: Optional[str]) -> bool:
    cleaned = clean_optional_text(value)
    return not cleaned or cleaned.upper() == "UNKNOWN"


def _resolve_display_name(name: Optional[str], account: Optional[str]) -> Optional[str]:
    raw_name = _fix_mojibake(clean_optional_text(name) or "")
    raw_account = clean_optional_text(account) or ""
    if raw_name and not _is_unknown_name(raw_name):
        return raw_name
    if raw_account:
        return raw_account
    return None


def _display_name_expr(name_col, account_col):
    trimmed_name = func.trim(name_col)
    trimmed_account = func.trim(account_col)
    invalid_name = func.lower(func.coalesce(trimmed_name, "")).in_(
        ["nan", "none", "null", "nat", "<na>", "n/a", "unknown"]
    )
    return case(
        (or_(trimmed_name.is_(None), trimmed_name == "", invalid_name), trimmed_account),
        else_=trimmed_name,
    )


def _normalized_account_expr(account_col):
    base = func.upper(func.coalesce(account_col, ""))
    base = func.replace(base, " ", "")
    base = func.replace(base, "-", "")
    base = func.replace(base, "/", "")
    return base


def _counterparty_key(iin_bin: Optional[str], account: Optional[str], display_name: Optional[str]) -> str:
    iin = _normalize_iin(iin_bin or "")
    acc = _normalize_account(account or "")
    name = _normalize_name(display_name or "")

    # Prefer IIN/BIN as stable person/company identifier regardless of account.
    if iin and iin not in {"000000000000", "0"}:
        return f"iin:{iin}"
    if acc:
        return f"acc:{acc}"
    return f"name:{name}"


def _pick_better_display_name(current: str, candidate: str) -> str:
    c = _fix_mojibake((current or "").strip())
    n = _fix_mojibake((candidate or "").strip())
    if not c:
        return n
    if not n:
        return c
    return n if len(n) > len(c) else c


def _date_range_conditions(date_from: Optional[str], date_to: Optional[str]):
    """Return a list of SQLAlchemy conditions for the date range."""
    conds = []
    effective_dt = _effective_dt_expr()
    if date_from:
        conds.append(effective_dt >= _parse_date(date_from))
    if date_to:
        dt = _parse_date(date_to).replace(hour=23, minute=59, second=59)
        conds.append(effective_dt <= dt)
    return conds


def _effective_dt_expr():
    return func.coalesce(Transaction.date, cast(Transaction.operation_date, DateTime))


def _text_match_conditions(column, query: str):
    q = (query or "").strip()
    if not q:
        return None
    variants = {q, q.lower(), q.upper(), q.capitalize(), q.title()}
    like_conds = [column.ilike(f"%{v}%") for v in variants if v]
    return or_(*like_conds) if like_conds else None


def _non_empty_text_condition(column):
    return func.nullif(func.trim(func.coalesce(column, "")), "").isnot(None)


def _meaningful_transaction_condition():
    return or_(
        Transaction.date.isnot(None),
        Transaction.operation_date.isnot(None),
        func.coalesce(Transaction.amount_tenge, 0) > 0,
        func.coalesce(Transaction.debit, 0) > 0,
        func.coalesce(Transaction.credit, 0) > 0,
        _non_empty_text_condition(Transaction.sender_name),
        _non_empty_text_condition(Transaction.sender_account),
        _non_empty_text_condition(Transaction.recipient_name),
        _non_empty_text_condition(Transaction.recipient_account),
        _non_empty_text_condition(Transaction.purpose),
        _non_empty_text_condition(Transaction.currency),
    )


def _shared_filter_conditions(
    *,
    date: Optional[str],
    date_from: Optional[str],
    date_to: Optional[str],
    category: Optional[str],
    search: Optional[str],
    min_amount: Optional[float],
    max_amount: Optional[float],
    currency: Optional[str],
    sender: Optional[str],
    recipient: Optional[str],
):
    conditions = [_meaningful_transaction_condition(), *_date_range_conditions(date_from, date_to)]

    if date:
        dt = _parse_date(date)
        conditions.append(
            or_(
                cast(func.timezone("UTC", Transaction.date), Date) == dt.date(),
                Transaction.operation_date == dt.date(),
            )
        )

    if category:
        category_q = category.strip()
        category_mojibake = _to_mojibake(category_q)
        category_expr = _derived_category_expr()
        cond_main = _text_match_conditions(category_expr, category_q)
        if cond_main is not None:
            if category_mojibake and category_mojibake != category_q:
                cond_moji = _text_match_conditions(category_expr, category_mojibake)
                conditions.append(or_(cond_main, cond_moji) if cond_moji is not None else cond_main)
            else:
                conditions.append(cond_main)

    if search:
        search_cond = _text_match_conditions(Transaction.purpose, search)
        if search_cond is not None:
            conditions.append(search_cond)
    if min_amount is not None:
        conditions.append(Transaction.amount_tenge >= min_amount)
    if max_amount is not None:
        conditions.append(Transaction.amount_tenge <= max_amount)
    if currency:
        conditions.append(Transaction.currency == currency.upper())
    if sender:
        sender_q = sender.strip()
        sender_name_cond = _text_match_conditions(Transaction.sender_name, sender_q)
        sender_match_list = [
            Transaction.sender_iin_bin.ilike(f"%{sender_q}%"),
            Transaction.sender_account.ilike(f"%{sender_q}%"),
        ]
        if sender_name_cond is not None:
            sender_match_list.insert(0, sender_name_cond)
        conditions.append(or_(*sender_match_list))
    if recipient:
        recipient_q = recipient.strip()
        recipient_name_cond = _text_match_conditions(Transaction.recipient_name, recipient_q)
        recipient_match_list = [
            Transaction.recipient_iin_bin.ilike(f"%{recipient_q}%"),
            Transaction.recipient_account.ilike(f"%{recipient_q}%"),
        ]
        if recipient_name_cond is not None:
            recipient_match_list.insert(0, recipient_name_cond)
        conditions.append(or_(*recipient_match_list))

    return conditions


def _effective_dt_value(tx: Transaction) -> Optional[datetime]:
    if tx.date is not None:
        return tx.date
    if tx.operation_date is not None:
        return datetime.combine(tx.operation_date, datetime.min.time())
    return None


def _format_tx_dt(tx: Transaction) -> str:
    dt = _effective_dt_value(tx)
    return dt.strftime("%d.%m.%Y %H:%M") if dt else ""


def _project_where(project_id: str, *conds):
    return and_(Transaction.project_id == project_id, *conds)


def _safe_delta_percent(value_a: float, value_b: float) -> Optional[float]:
    if abs(value_a) < 1e-9:
        return 0.0 if abs(value_b) < 1e-9 else None
    return round(((value_b - value_a) / value_a) * 100, 2)


async def _count_unique_counterparties(
    db: AsyncSession,
    *,
    project_id: str,
    shared_conds: list,
) -> int:
    sender_display = _display_name_expr(Transaction.sender_name, Transaction.sender_account)
    sender_iin = func.upper(func.coalesce(Transaction.sender_iin_bin, ""))
    sender_acc = _normalized_account_expr(Transaction.sender_account)
    sender_has_iin = and_(sender_iin != "", sender_iin != "0", sender_iin != "000000000000")
    sender_has_acc = sender_acc != ""
    sender_key = case(
        (sender_has_iin, literal("iin:") + sender_iin),
        (sender_has_acc, literal("acc:") + sender_acc),
        else_=literal("name:") + sender_display,
    )

    recipient_display = _display_name_expr(Transaction.recipient_name, Transaction.recipient_account)
    recipient_iin = func.upper(func.coalesce(Transaction.recipient_iin_bin, ""))
    recipient_acc = _normalized_account_expr(Transaction.recipient_account)
    recipient_has_iin = and_(recipient_iin != "", recipient_iin != "0", recipient_iin != "000000000000")
    recipient_has_acc = recipient_acc != ""
    recipient_key = case(
        (recipient_has_iin, literal("iin:") + recipient_iin),
        (recipient_has_acc, literal("acc:") + recipient_acc),
        else_=literal("name:") + recipient_display,
    )

    sender_side = (
        select(sender_key.label("cp_key"))
        .where(
            _project_where(
                project_id,
                sender_display.isnot(None),
                sender_display != "",
                *shared_conds,
            )
        )
    )
    recipient_side = (
        select(recipient_key.label("cp_key"))
        .where(
            _project_where(
                project_id,
                recipient_display.isnot(None),
                recipient_display != "",
                *shared_conds,
            )
        )
    )

    combined = union_all(sender_side, recipient_side).subquery()
    q = select(func.count(func.distinct(combined.c.cp_key)))
    return int((await db.execute(q)).scalar() or 0)


async def _build_compare_summary(
    db: AsyncSession,
    *,
    project_id: str,
    shared_conds: list,
) -> ComparePeriodSummary:
    row = (
        await db.execute(
            select(
                func.coalesce(func.sum(Transaction.credit), 0).label("total_credit"),
                func.coalesce(func.sum(Transaction.debit), 0).label("total_debit"),
                func.count(Transaction.id).label("total_transactions"),
            ).where(_project_where(project_id, *shared_conds))
        )
    ).one()

    unique_counterparties = await _count_unique_counterparties(
        db,
        project_id=project_id,
        shared_conds=shared_conds,
    )

    return ComparePeriodSummary(
        total_debit=float(row.total_debit or 0),
        total_credit=float(row.total_credit or 0),
        total_transactions=int(row.total_transactions or 0),
        unique_counterparties=unique_counterparties,
    )


async def _build_compare_categories(
    db: AsyncSession,
    *,
    project_id: str,
    shared_conds_a: list,
    shared_conds_b: list,
    limit: int,
) -> list[ComparePeriodCategoryItem]:
    category_expr = _derived_category_expr()

    async def fetch_rows(shared_conds: list):
        query = (
            select(
                category_expr.label("category"),
                func.count(Transaction.id).label("tx_count"),
                func.coalesce(func.sum(Transaction.amount_tenge), 0).label("turnover"),
            )
            .where(_project_where(project_id, category_expr.isnot(None), *shared_conds))
            .group_by(category_expr)
        )
        return (await db.execute(query)).all()

    rows_a = await fetch_rows(shared_conds_a)
    rows_b = await fetch_rows(shared_conds_b)

    merged: dict[str, dict] = {}

    for row in rows_a:
        category = _fix_mojibake((row.category or "").strip())
        if not category:
            continue
        merged.setdefault(
            category,
            {"value_a": 0.0, "value_b": 0.0, "tx_a": 0, "tx_b": 0},
        )
        merged[category]["value_a"] += float(row.turnover or 0)
        merged[category]["tx_a"] += int(row.tx_count or 0)

    for row in rows_b:
        category = _fix_mojibake((row.category or "").strip())
        if not category:
            continue
        merged.setdefault(
            category,
            {"value_a": 0.0, "value_b": 0.0, "tx_a": 0, "tx_b": 0},
        )
        merged[category]["value_b"] += float(row.turnover or 0)
        merged[category]["tx_b"] += int(row.tx_count or 0)

    items = [
        ComparePeriodCategoryItem(
            category=category,
            value_a=values["value_a"],
            value_b=values["value_b"],
            delta=values["value_b"] - values["value_a"],
            delta_percent=_safe_delta_percent(values["value_a"], values["value_b"]),
            transaction_count_a=values["tx_a"],
            transaction_count_b=values["tx_b"],
        )
        for category, values in merged.items()
    ]

    items.sort(key=lambda item: (abs(item.delta), item.value_b, item.value_a), reverse=True)
    return items[:limit]


def _build_compare_metrics(
    summary_a: ComparePeriodSummary,
    summary_b: ComparePeriodSummary,
) -> list[ComparePeriodMetric]:
    metric_specs = [
        ("Общий дебет", float(summary_a.total_debit), float(summary_b.total_debit)),
        ("Общий кредит", float(summary_a.total_credit), float(summary_b.total_credit)),
        ("Кол-во транзакций", float(summary_a.total_transactions), float(summary_b.total_transactions)),
        ("Уникальные контрагенты", float(summary_a.unique_counterparties), float(summary_b.unique_counterparties)),
    ]

    return [
        ComparePeriodMetric(
            label=label,
            value_a=value_a,
            value_b=value_b,
            delta=ComparePeriodDelta(
                absolute=value_b - value_a,
                percent=_safe_delta_percent(value_a, value_b),
            ),
        )
        for label, value_a, value_b in metric_specs
    ]


def _build_compare_anomalies(
    summary_a: ComparePeriodSummary,
    summary_b: ComparePeriodSummary,
    categories: list[ComparePeriodCategoryItem],
) -> list[str]:
    anomalies: list[str] = []

    debit_pct = _safe_delta_percent(summary_a.total_debit, summary_b.total_debit)
    if debit_pct is not None and debit_pct >= 50:
        anomalies.append(f"Расходы выросли на {round(debit_pct)}% в периоде B.")

    tx_pct = _safe_delta_percent(float(summary_a.total_transactions), float(summary_b.total_transactions))
    if tx_pct is not None and tx_pct >= 30:
        anomalies.append(f"Количество транзакций выросло на {round(tx_pct)}%.")

    unique_delta = summary_b.unique_counterparties - summary_a.unique_counterparties
    if unique_delta >= 5:
        anomalies.append(f"Появилось {unique_delta} дополнительных уникальных контрагентов.")

    cash_row = next((item for item in categories if item.category == "Снятие наличных"), None)
    if cash_row and cash_row.delta_percent is not None and cash_row.delta_percent >= 100:
        anomalies.append(f"Снятие наличных выросло на {round(cash_row.delta_percent)}%.")

    new_category = next(
        (
            item for item in categories
            if item.value_a <= 0 and item.value_b > 0
        ),
        None,
    )
    if new_category:
        anomalies.append(f"В периоде B появилась новая заметная категория: {new_category.category}.")

    return anomalies[:4]


def _cash_withdrawal_condition():
    purpose = func.lower(func.coalesce(Transaction.purpose, ""))
    category = func.lower(func.coalesce(Transaction.category, ""))
    op_type = func.lower(func.coalesce(Transaction.operation_type, ""))
    purpose_raw = func.coalesce(Transaction.purpose, "")
    category_raw = func.coalesce(Transaction.category, "")
    op_type_raw = func.coalesce(Transaction.operation_type, "")
    return or_(
        purpose.like("%cash%"),
        purpose.like("%atm%"),
        purpose_raw.like("%\u0441\u043d\u044f\u0442\u0438\u0435%"),
        purpose_raw.like("%\u0421\u041d\u042f\u0422\u0418\u0415%"),
        purpose_raw.like("%\u043d\u0430\u043b\u0438\u0447%"),
        purpose_raw.like("%\u041d\u0410\u041b\u0418\u0427%"),
        category_raw.like("%\u0441\u043d\u044f\u0442\u0438\u0435%"),
        category_raw.like("%\u0421\u043d\u044f\u0442\u0438\u0435%"),
        category_raw.like("%\u043d\u0430\u043b\u0438\u0447%"),
        category_raw.like("%\u041d\u0410\u041b\u0418\u0427%"),
        category.like("%atm%"),
        op_type_raw.like("%\u0441\u043d\u044f\u0442\u0438\u0435%"),
        op_type_raw.like("%\u0421\u043d\u044f\u0442\u0438\u0435%"),
        op_type_raw.like("%\u043d\u0430\u043b\u0438\u0447%"),
        op_type_raw.like("%\u041d\u0410\u041b\u0418\u0427%"),
        op_type_raw.like("%\u0431\u0430\u043d\u043a\u043e\u043c\u0430\u0442%"),
        op_type_raw.like("%\u0411\u0430\u043d\u043a\u043e\u043c\u0430\u0442%"),
        op_type.like("%atm%"),
        op_type_raw.like("%\u0438\u0441\u0445.\u0434\u043e\u043a%"),
        op_type_raw.like("%\u0418\u0421\u0425%"),
    )


def _cash_deposit_condition():
    purpose = func.lower(func.coalesce(Transaction.purpose, ""))
    category = func.lower(func.coalesce(Transaction.category, ""))
    op_type = func.lower(func.coalesce(Transaction.operation_type, ""))
    purpose_raw = func.coalesce(Transaction.purpose, "")
    category_raw = func.coalesce(Transaction.category, "")
    op_type_raw = func.coalesce(Transaction.operation_type, "")
    return or_(
        purpose.like("%deposit%"),
        purpose_raw.like("%\u0432\u0437\u043d\u043e\u0441%"),
        purpose_raw.like("%\u0412\u0437\u043d\u043e\u0441%"),
        purpose_raw.like("%\u043f\u043e\u043f\u043e\u043b\u043d%"),
        purpose_raw.like("%\u041f\u043e\u043f\u043e\u043b\u043d%"),
        purpose.like("%cash in%"),
        category.like("%deposit%"),
        category_raw.like("%\u0432\u0437\u043d\u043e\u0441%"),
        category_raw.like("%\u0412\u0437\u043d\u043e\u0441%"),
        category_raw.like("%\u043f\u043e\u043f\u043e\u043b\u043d%"),
        category_raw.like("%\u041f\u043e\u043f\u043e\u043b\u043d%"),
        category_raw.like("%\u043d\u0430\u043b\u0438\u0447%"),
        category_raw.like("%\u041d\u0430\u043b\u0438\u0447%"),
        op_type_raw.like("%\u0432\u0445.\u0434\u043e\u043a%"),
        op_type_raw.like("%\u0412\u0425%"),
        op_type_raw.like("%\u043a\u0440\u0435\u0434\u0438\u0442%"),
        op_type_raw.like("%\u041a\u0420\u0415\u0414\u0418\u0422%"),
        op_type_raw.like("%\u0432\u0437\u043d\u043e\u0441%"),
        op_type_raw.like("%\u0412\u0437\u043d\u043e\u0441%"),
        op_type.like("%deposit%"),
    )


# -------------------------------------------------------------------------
# 1. Time-Series
# -------------------------------------------------------------------------

@router.get("/time-series", response_model=TimeSeriesResponse)
async def time_series(
    period: str = Query("month", description="year | month | day"),
    date: Optional[str] = Query(None, description="Exact date DD.MM.YYYY"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    min_amount: Optional[float] = Query(None),
    max_amount: Optional[float] = Query(None),
    currency: Optional[str] = Query(None),
    sender: Optional[str] = Query(None),
    recipient: Optional[str] = Query(None),
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    conds = _shared_filter_conditions(
        date=date,
        date_from=date_from,
        date_to=date_to,
        category=category,
        search=search,
        min_amount=min_amount,
        max_amount=max_amount,
        currency=currency,
        sender=sender,
        recipient=recipient,
    )
    where = _project_where(ctx.project.project_id, *conds)

    # Group expression depends on period
    effective_dt = _effective_dt_expr()
    if period == "year":
        group_expr = func.to_char(effective_dt, "YYYY")
    elif period == "day":
        group_expr = func.to_char(effective_dt, "YYYY-MM-DD")
    else:  # month (default)
        group_expr = func.to_char(effective_dt, "YYYY-MM")

    q = (
        select(
            group_expr.label("bucket"),
            func.coalesce(func.sum(Transaction.credit), 0).label("credit"),
            func.coalesce(func.sum(Transaction.debit), 0).label("debit"),
        )
        .where(where)
        .group_by("bucket")
        .order_by("bucket")
    )

    rows = (await db.execute(q)).all()

    data = []
    for row in rows:
        bucket = row.bucket
        if not bucket:
            continue
        if period == "year":
            label = bucket  # "2026"
        elif period == "day":
            dt = datetime.strptime(bucket, "%Y-%m-%d")
            label = dt.strftime("%d.%m.%Y")
        else:
            dt = datetime.strptime(bucket, "%Y-%m")
            label = f"{MONTHS_RU[dt.month - 1]} {dt.year}"

        data.append(
            TimeSeriesPoint(
                label=label,
                date=bucket,
                credit=float(row.credit),
                debit=float(row.debit),
            )
        )

    return TimeSeriesResponse(period=period, data=data)


@router.get("/time-series-transactions", response_model=TimeSeriesTransactionsResponse)
async def time_series_transactions(
    period: str = Query("month", description="year | month | day"),
    bucket: str = Query(..., description="YYYY | YYYY-MM | YYYY-MM-DD"),
    limit: int = Query(200, ge=1, le=500),
    date: Optional[str] = Query(None, description="Exact date DD.MM.YYYY"),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    min_amount: Optional[float] = Query(None),
    max_amount: Optional[float] = Query(None),
    currency: Optional[str] = Query(None),
    sender: Optional[str] = Query(None),
    recipient: Optional[str] = Query(None),
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    effective_dt = _effective_dt_expr()
    if period == "year":
        cond = (func.to_char(effective_dt, "YYYY") == bucket)
    elif period == "day":
        cond = (func.to_char(effective_dt, "YYYY-MM-DD") == bucket)
    else:
        cond = (func.to_char(effective_dt, "YYYY-MM") == bucket)

    where = _project_where(
        ctx.project.project_id,
        cond,
        *_shared_filter_conditions(
            date=date,
            date_from=None,
            date_to=None,
            category=category,
            search=search,
            min_amount=min_amount,
            max_amount=max_amount,
            currency=currency,
            sender=sender,
            recipient=recipient,
        ),
    )

    total_q = select(func.count(Transaction.id)).where(where)
    total = int((await db.execute(total_q)).scalar() or 0)

    rows_q = (
        select(Transaction)
        .where(where)
        .order_by(effective_dt.desc(), Transaction.id.desc())
        .limit(limit)
    )
    rows = (await db.execute(rows_q)).scalars().all()

    data = [
        CashTransactionItem(
            id=str(t.id),
            date=_format_tx_dt(t),
            sender_name=_resolve_display_name(t.sender_name, t.sender_account) or "—",
            recipient_name=_resolve_display_name(t.recipient_name, t.recipient_account) or "—",
            purpose=_fix_mojibake(t.purpose or ""),
            currency=t.currency or "",
            debit=float(t.debit or 0),
            credit=float(t.credit or 0),
            amount_tenge=float(t.amount_tenge or 0),
        )
        for t in rows
    ]

    return TimeSeriesTransactionsResponse(
        period=period,
        bucket=bucket,
        total=total,
        data=data,
    )


# -------------------------------------------------------------------------
# 2. Summary (KPI cards)
# -------------------------------------------------------------------------

@router.get("/summary", response_model=AnalyticsSummaryResponse)
async def analytics_summary(
    date: Optional[str] = Query(None, description="Exact date DD.MM.YYYY"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    min_amount: Optional[float] = Query(None),
    max_amount: Optional[float] = Query(None),
    currency: Optional[str] = Query(None),
    sender: Optional[str] = Query(None),
    recipient: Optional[str] = Query(None),
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    conds = _shared_filter_conditions(
        date=date,
        date_from=date_from,
        date_to=date_to,
        category=category,
        search=search,
        min_amount=min_amount,
        max_amount=max_amount,
        currency=currency,
        sender=sender,
        recipient=recipient,
    )
    where = _project_where(ctx.project.project_id, *conds)

    q = select(
        func.coalesce(func.sum(Transaction.credit), 0).label("total_credit"),
        func.coalesce(func.sum(Transaction.debit), 0).label("total_debit"),
        func.count(Transaction.id).label("total_transactions"),
        func.min(_effective_dt_expr()).label("min_date"),
        func.max(_effective_dt_expr()).label("max_date"),
    ).where(where)

    row = (await db.execute(q)).one()
    total_credit = float(row.total_credit)
    total_debit = float(row.total_debit)

    period_from = row.min_date.strftime("%d.%m.%Y") if row.min_date else ""
    period_to = row.max_date.strftime("%d.%m.%Y") if row.max_date else ""

    return AnalyticsSummaryResponse(
        total_credit=total_credit,
        total_debit=total_debit,
        total_turnover=total_credit + total_debit,
        total_transactions=row.total_transactions,
        period=PeriodRange(from_=period_from, to=period_to),
    )


@router.get("/compare-periods", response_model=ComparePeriodsResponse)
async def compare_periods(
    date_from_a: str = Query(..., description="Start date for period A in DD.MM.YYYY"),
    date_to_a: str = Query(..., description="End date for period A in DD.MM.YYYY"),
    date_from_b: str = Query(..., description="Start date for period B in DD.MM.YYYY"),
    date_to_b: str = Query(..., description="End date for period B in DD.MM.YYYY"),
    limit: int = Query(20, ge=1, le=100),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    min_amount: Optional[float] = Query(None),
    max_amount: Optional[float] = Query(None),
    currency: Optional[str] = Query(None),
    sender: Optional[str] = Query(None),
    recipient: Optional[str] = Query(None),
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    if _parse_date(date_from_a) > _parse_date(date_to_a):
        raise HTTPException(status_code=422, detail="date_from_a must be <= date_to_a")
    if _parse_date(date_from_b) > _parse_date(date_to_b):
        raise HTTPException(status_code=422, detail="date_from_b must be <= date_to_b")

    shared_conds_a = _shared_filter_conditions(
        date=None,
        date_from=date_from_a,
        date_to=date_to_a,
        category=category,
        search=search,
        min_amount=min_amount,
        max_amount=max_amount,
        currency=currency,
        sender=sender,
        recipient=recipient,
    )
    shared_conds_b = _shared_filter_conditions(
        date=None,
        date_from=date_from_b,
        date_to=date_to_b,
        category=category,
        search=search,
        min_amount=min_amount,
        max_amount=max_amount,
        currency=currency,
        sender=sender,
        recipient=recipient,
    )

    summary_a = await _build_compare_summary(
        db,
        project_id=ctx.project.project_id,
        shared_conds=shared_conds_a,
    )
    summary_b = await _build_compare_summary(
        db,
        project_id=ctx.project.project_id,
        shared_conds=shared_conds_b,
    )
    categories = await _build_compare_categories(
        db,
        project_id=ctx.project.project_id,
        shared_conds_a=shared_conds_a,
        shared_conds_b=shared_conds_b,
        limit=limit,
    )
    metrics = _build_compare_metrics(summary_a, summary_b)
    anomalies = _build_compare_anomalies(summary_a, summary_b, categories)

    return ComparePeriodsResponse(
        period_a=PeriodRange(from_=date_from_a, to=date_to_a),
        period_b=PeriodRange(from_=date_from_b, to=date_to_b),
        summary_a=summary_a,
        summary_b=summary_b,
        metrics=metrics,
        categories=categories,
        anomalies=anomalies,
    )


# -------------------------------------------------------------------------
# 3. Top Expenses / Receipts
# -------------------------------------------------------------------------

@router.get("/top-expenses", response_model=TopExpensesResponse)
async def top_expenses(
    type: str = Query("debit", description="debit | credit"),
    limit: int = Query(10, ge=1, le=100),
    date: Optional[str] = Query(None, description="Exact date DD.MM.YYYY"),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    min_amount: Optional[float] = Query(None),
    max_amount: Optional[float] = Query(None),
    currency: Optional[str] = Query(None),
    sender: Optional[str] = Query(None),
    recipient: Optional[str] = Query(None),
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    if type == "credit":
        amount_col = Transaction.credit
        group_name = Transaction.recipient_name
        group_iin = Transaction.recipient_iin_bin
        group_acc = Transaction.recipient_account
    else:
        amount_col = Transaction.debit
        group_name = Transaction.sender_name
        group_iin = Transaction.sender_iin_bin
        group_acc = Transaction.sender_account
    display_name = _display_name_expr(group_name, group_acc)
    valid_display = and_(display_name.isnot(None), display_name != "")

    shared_conds = _shared_filter_conditions(
        date=date,
        date_from=None,
        date_to=None,
        category=category,
        search=search,
        min_amount=min_amount,
        max_amount=max_amount,
        currency=currency,
        sender=sender,
        recipient=recipient,
    )
    q = (
        select(
            display_name.label("cp_name"),
            group_iin.label("cp_iin"),
            group_acc.label("cp_acc"),
            func.sum(amount_col).label("amount"),
            func.count(Transaction.id).label("tx_count"),
        )
        .where(_project_where(ctx.project.project_id, amount_col > 0, valid_display, *shared_conds))
        .group_by(display_name, group_iin, group_acc)
        .order_by(func.sum(amount_col).desc())
        .limit(limit)
    )

    rows = (await db.execute(q)).all()

    # grand total for percentage
    total_q = select(func.coalesce(func.sum(amount_col), 0)).where(
        _project_where(ctx.project.project_id, amount_col > 0, valid_display, *shared_conds)
    )
    grand_total = float((await db.execute(total_q)).scalar() or 0)

    merged: dict[str, dict] = {}
    for r in rows:
        cp_name = _fix_mojibake(r.cp_name or "")
        cp_iin = _normalize_iin(r.cp_iin or "")
        cp_acc = _normalize_account(r.cp_acc or "")
        key = _counterparty_key(cp_iin, cp_acc, cp_name)
        item = merged.get(key)
        if not item:
            merged[key] = {
                "name": cp_name,
                "iin": cp_iin,
                "acc": "" if (cp_iin and cp_iin not in {"0", "000000000000"}) else cp_acc,
                "amount": float(r.amount or 0),
                "tx_count": int(r.tx_count or 0),
            }
            continue
        item["name"] = _pick_better_display_name(item["name"], cp_name)
        item["amount"] += float(r.amount or 0)
        item["tx_count"] += int(r.tx_count or 0)

    data = [
        TopExpenseItem(
            counterparty=CounterpartyOut(
                name=v["name"], iin_bin=v["iin"], account=v["acc"],
            ),
            amount=float(v["amount"]),
            transaction_count=int(v["tx_count"]),
            percentage=round(float(v["amount"]) / grand_total * 100, 2) if grand_total else 0,
        )
        for v in sorted(merged.values(), key=lambda x: x["amount"], reverse=True)[:limit]
    ]

    return TopExpensesResponse(type=type, total=grand_total, data=data)


@router.get("/top-expenses-transactions", response_model=CounterpartyTransactionsResponse)
async def top_expenses_transactions(
    type: str = Query("debit", description="debit | credit"),
    iin_bin: Optional[str] = Query(None),
    account: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=500),
    date: Optional[str] = Query(None, description="Exact date DD.MM.YYYY"),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    min_amount: Optional[float] = Query(None),
    max_amount: Optional[float] = Query(None),
    currency: Optional[str] = Query(None),
    sender: Optional[str] = Query(None),
    recipient: Optional[str] = Query(None),
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    norm_iin = _normalize_iin(iin_bin or "")
    norm_acc = _normalize_account(account or "")
    cp_name = _fix_mojibake((name or "").strip())

    if not norm_iin and not norm_acc and not cp_name:
        raise MissingIdentityFieldsException

    if type == "credit":
        amount_col = Transaction.credit
        cp_name_col = Transaction.recipient_name
        cp_iin_col = Transaction.recipient_iin_bin
        cp_acc_col = Transaction.recipient_account
    else:
        amount_col = Transaction.debit
        cp_name_col = Transaction.sender_name
        cp_iin_col = Transaction.sender_iin_bin
        cp_acc_col = Transaction.sender_account

    if norm_iin and norm_iin not in {"0", "000000000000"}:
        cp_cond = (cp_iin_col == norm_iin)
    elif norm_acc:
        cp_cond = (_normalized_account_expr(cp_acc_col) == norm_acc)
    else:
        cp_cond = (_display_name_expr(cp_name_col, cp_acc_col) == cp_name)

    where = _project_where(
        ctx.project.project_id,
        amount_col > 0,
        cp_cond,
        *_shared_filter_conditions(
            date=date,
            date_from=None,
            date_to=None,
            category=category,
            search=search,
            min_amount=min_amount,
            max_amount=max_amount,
            currency=currency,
            sender=sender,
            recipient=recipient,
        ),
    )
    total_q = select(func.count(Transaction.id)).where(where)
    total = int((await db.execute(total_q)).scalar() or 0)

    rows_q = (
        select(Transaction)
        .where(where)
        .order_by(_effective_dt_expr().desc(), Transaction.id.desc())
        .limit(limit)
    )
    rows = (await db.execute(rows_q)).scalars().all()

    data = [
        CashTransactionItem(
            id=str(t.id),
            date=_format_tx_dt(t),
            sender_name=_resolve_display_name(t.sender_name, t.sender_account) or "—",
            recipient_name=_resolve_display_name(t.recipient_name, t.recipient_account) or "—",
            purpose=_fix_mojibake(t.purpose or ""),
            currency=t.currency or "",
            debit=float(t.debit or 0),
            credit=float(t.credit or 0),
            amount_tenge=float(t.amount_tenge or 0),
        )
        for t in rows
    ]

    display_name = cp_name
    if not display_name and rows:
        if type == "credit":
            display_name = _resolve_display_name(rows[0].recipient_name, rows[0].recipient_account) or "—"
        else:
            display_name = _resolve_display_name(rows[0].sender_name, rows[0].sender_account) or "—"

    return CounterpartyTransactionsResponse(
        counterparty=CounterpartyOut(name=display_name or "—", iin_bin=norm_iin, account=norm_acc),
        total=total,
        data=data,
    )


# -------------------------------------------------------------------------
# 4. Top Counterparties (by turnover)
# -------------------------------------------------------------------------

@router.get("/top-counterparties", response_model=TopCounterpartiesResponse)
async def top_counterparties(
    limit: int = Query(10, ge=1, le=100),
    date: Optional[str] = Query(None, description="Exact date DD.MM.YYYY"),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    min_amount: Optional[float] = Query(None),
    max_amount: Optional[float] = Query(None),
    currency: Optional[str] = Query(None),
    sender: Optional[str] = Query(None),
    recipient: Optional[str] = Query(None),
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    shared_conds = _shared_filter_conditions(
        date=date,
        date_from=None,
        date_to=None,
        category=category,
        search=search,
        min_amount=min_amount,
        max_amount=max_amount,
        currency=currency,
        sender=sender,
        recipient=recipient,
    )
    sender_display = _display_name_expr(Transaction.sender_name, Transaction.sender_account)
    sender_iin = func.upper(func.coalesce(Transaction.sender_iin_bin, ""))
    sender_acc = _normalized_account_expr(Transaction.sender_account)
    sender_has_iin = and_(sender_iin != "", sender_iin != "0", sender_iin != "000000000000")
    sender_has_acc = sender_acc != ""
    sender_key = case(
        (sender_has_iin, literal("iin:") + sender_iin),
        (sender_has_acc, literal("acc:") + sender_acc),
        else_=literal("name:") + sender_display,
    )
    sender_account_out = case((sender_has_iin, literal("")), else_=sender_acc)
    sender_iin_out = case((sender_has_iin, sender_iin), else_=literal(""))

    recipient_display = _display_name_expr(Transaction.recipient_name, Transaction.recipient_account)
    recipient_iin = func.upper(func.coalesce(Transaction.recipient_iin_bin, ""))
    recipient_acc = _normalized_account_expr(Transaction.recipient_account)
    recipient_has_iin = and_(recipient_iin != "", recipient_iin != "0", recipient_iin != "000000000000")
    recipient_has_acc = recipient_acc != ""
    recipient_key = case(
        (recipient_has_iin, literal("iin:") + recipient_iin),
        (recipient_has_acc, literal("acc:") + recipient_acc),
        else_=literal("name:") + recipient_display,
    )
    recipient_account_out = case((recipient_has_iin, literal("")), else_=recipient_acc)
    recipient_iin_out = case((recipient_has_iin, recipient_iin), else_=literal(""))

    sender_side = (
        select(
            sender_key.label("cp_key"),
            sender_display.label("cp_name"),
            sender_iin_out.label("iin_bin"),
            sender_account_out.label("account"),
            literal(0.0).label("total_credit"),
            func.coalesce(func.sum(Transaction.debit), 0).label("total_debit"),
            func.coalesce(func.sum(Transaction.amount_tenge), 0).label("total_turnover"),
            func.count(Transaction.id).label("transaction_count"),
        )
        .where(
            _project_where(
                ctx.project.project_id,
                sender_display.isnot(None),
                sender_display != "",
                or_(sender_has_iin, sender_has_acc),
                *shared_conds,
            )
        )
        .group_by(sender_key, sender_display, sender_iin_out, sender_account_out)
    )

    recipient_side = (
        select(
            recipient_key.label("cp_key"),
            recipient_display.label("cp_name"),
            recipient_iin_out.label("iin_bin"),
            recipient_account_out.label("account"),
            func.coalesce(func.sum(Transaction.credit), 0).label("total_credit"),
            literal(0.0).label("total_debit"),
            func.coalesce(func.sum(Transaction.amount_tenge), 0).label("total_turnover"),
            func.count(Transaction.id).label("transaction_count"),
        )
        .where(
            _project_where(
                ctx.project.project_id,
                recipient_display.isnot(None),
                recipient_display != "",
                or_(recipient_has_iin, recipient_has_acc),
                *shared_conds,
            )
        )
        .group_by(recipient_key, recipient_display, recipient_iin_out, recipient_account_out)
    )

    combined = union_all(sender_side, recipient_side).subquery()
    q = (
        select(
            func.max(combined.c.cp_name).label("cp_name"),
            func.max(combined.c.iin_bin).label("iin_bin"),
            func.max(combined.c.account).label("account"),
            func.coalesce(func.sum(combined.c.total_credit), 0).label("total_credit"),
            func.coalesce(func.sum(combined.c.total_debit), 0).label("total_debit"),
            func.coalesce(func.sum(combined.c.total_turnover), 0).label("total_turnover"),
            func.coalesce(func.sum(combined.c.transaction_count), 0).label("transaction_count"),
        )
        .group_by(combined.c.cp_key)
        .order_by(
            func.sum(combined.c.total_turnover).desc(),
            func.sum(combined.c.transaction_count).desc(),
        )
        .limit(limit)
    )
    rows = (await db.execute(q)).all()

    results = [
        TopCounterpartyItem(
            counterparty=CounterpartyOut(
                name=_fix_mojibake(r.cp_name or "—"),
                iin_bin=r.iin_bin or "",
                account=r.account or "",
            ),
            total_credit=float(r.total_credit or 0),
            total_debit=float(r.total_debit or 0),
            total_turnover=float(r.total_turnover or 0),
            transaction_count=int(r.transaction_count or 0),
        )
        for r in rows
    ]

    return TopCounterpartiesResponse(data=results)


@router.get("/counterparty-search", response_model=CounterpartySearchResponse)
async def counterparty_search(
    q: str = Query(..., min_length=2, description="Partial counterparty name, IIN/BIN or account"),
    limit: int = Query(8, ge=1, le=50),
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    query_text = (q or "").strip()
    lowered_like = f"%{query_text.lower()}%"
    norm_iin = _normalize_iin(query_text)
    norm_acc = _normalize_account(query_text)

    sender_display = _display_name_expr(Transaction.sender_name, Transaction.sender_account)
    sender_iin = func.upper(func.coalesce(Transaction.sender_iin_bin, ""))
    sender_acc = _normalized_account_expr(Transaction.sender_account)
    sender_has_iin = and_(sender_iin != "", sender_iin != "0", sender_iin != "000000000000")
    sender_has_acc = sender_acc != ""
    sender_key = case(
        (sender_has_iin, literal("iin:") + sender_iin),
        (sender_has_acc, literal("acc:") + sender_acc),
        else_=literal("name:") + sender_display,
    )
    sender_account_out = case((sender_has_iin, literal("")), else_=sender_acc)
    sender_iin_out = case((sender_has_iin, sender_iin), else_=literal(""))
    sender_match_filters = [func.lower(sender_display).like(lowered_like)]
    if norm_iin:
        sender_match_filters.append(sender_iin.like(f"%{norm_iin}%"))
    if norm_acc:
        sender_match_filters.append(sender_acc.like(f"%{norm_acc}%"))

    recipient_display = _display_name_expr(Transaction.recipient_name, Transaction.recipient_account)
    recipient_iin = func.upper(func.coalesce(Transaction.recipient_iin_bin, ""))
    recipient_acc = _normalized_account_expr(Transaction.recipient_account)
    recipient_has_iin = and_(recipient_iin != "", recipient_iin != "0", recipient_iin != "000000000000")
    recipient_has_acc = recipient_acc != ""
    recipient_key = case(
        (recipient_has_iin, literal("iin:") + recipient_iin),
        (recipient_has_acc, literal("acc:") + recipient_acc),
        else_=literal("name:") + recipient_display,
    )
    recipient_account_out = case((recipient_has_iin, literal("")), else_=recipient_acc)
    recipient_iin_out = case((recipient_has_iin, recipient_iin), else_=literal(""))
    recipient_match_filters = [func.lower(recipient_display).like(lowered_like)]
    if norm_iin:
        recipient_match_filters.append(recipient_iin.like(f"%{norm_iin}%"))
    if norm_acc:
        recipient_match_filters.append(recipient_acc.like(f"%{norm_acc}%"))

    sender_side = (
        select(
            sender_key.label("cp_key"),
            sender_display.label("cp_name"),
            sender_iin_out.label("iin_bin"),
            sender_account_out.label("account"),
            func.coalesce(func.sum(Transaction.amount_tenge), 0).label("total_turnover"),
            func.count(Transaction.id).label("transaction_count"),
        )
        .where(
            _project_where(
                ctx.project.project_id,
                sender_display.isnot(None),
                sender_display != "",
                or_(sender_has_iin, sender_has_acc),
                or_(*sender_match_filters),
            )
        )
        .group_by(sender_key, sender_display, sender_iin_out, sender_account_out)
    )

    recipient_side = (
        select(
            recipient_key.label("cp_key"),
            recipient_display.label("cp_name"),
            recipient_iin_out.label("iin_bin"),
            recipient_account_out.label("account"),
            func.coalesce(func.sum(Transaction.amount_tenge), 0).label("total_turnover"),
            func.count(Transaction.id).label("transaction_count"),
        )
        .where(
            _project_where(
                ctx.project.project_id,
                recipient_display.isnot(None),
                recipient_display != "",
                or_(recipient_has_iin, recipient_has_acc),
                or_(*recipient_match_filters),
            )
        )
        .group_by(recipient_key, recipient_display, recipient_iin_out, recipient_account_out)
    )

    combined = union_all(sender_side, recipient_side).subquery()
    rows = (
        await db.execute(
            select(
                func.max(combined.c.cp_name).label("cp_name"),
                func.max(combined.c.iin_bin).label("iin_bin"),
                func.max(combined.c.account).label("account"),
                func.coalesce(func.sum(combined.c.total_turnover), 0).label("total_turnover"),
                func.coalesce(func.sum(combined.c.transaction_count), 0).label("transaction_count"),
            )
            .group_by(combined.c.cp_key)
            .order_by(
                func.sum(combined.c.total_turnover).desc(),
                func.sum(combined.c.transaction_count).desc(),
            )
            .limit(limit)
        )
    ).all()

    return CounterpartySearchResponse(
        data=[
            CounterpartySearchItem(
                counterparty=CounterpartyOut(
                    name=_fix_mojibake(row.cp_name or "—"),
                    iin_bin=row.iin_bin or "",
                    account=row.account or "",
                ),
                total_turnover=float(row.total_turnover or 0),
                transaction_count=int(row.transaction_count or 0),
            )
            for row in rows
        ]
    )


@router.get("/category-summary", response_model=CategorySummaryResponse)
async def category_summary(
    limit: int = Query(24, ge=1, le=100),
    date: Optional[str] = Query(None, description="Exact date DD.MM.YYYY"),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    min_amount: Optional[float] = Query(None),
    max_amount: Optional[float] = Query(None),
    currency: Optional[str] = Query(None),
    sender: Optional[str] = Query(None),
    recipient: Optional[str] = Query(None),
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    category_expr = _derived_category_expr()
    shared_conds = _shared_filter_conditions(
        date=date,
        date_from=None,
        date_to=None,
        category=category,
        search=search,
        min_amount=min_amount,
        max_amount=max_amount,
        currency=currency,
        sender=sender,
        recipient=recipient,
    )
    q = (
        select(
            category_expr.label("category"),
            func.count(Transaction.id).label("tx_count"),
            func.coalesce(func.sum(Transaction.amount_tenge), 0).label("turnover"),
            func.coalesce(func.sum(Transaction.debit), 0).label("total_debit"),
            func.coalesce(func.sum(Transaction.credit), 0).label("total_credit"),
        )
        .where(_project_where(ctx.project.project_id, category_expr.isnot(None), *shared_conds))
        .group_by(category_expr)
        .order_by(func.count(Transaction.id).desc(), func.sum(Transaction.amount_tenge).desc())
        .limit(limit)
    )
    rows = (await db.execute(q)).all()

    data = [
        CategorySummaryItem(
            category=_fix_mojibake((r.category or "").strip()),
            transaction_count=int(r.tx_count or 0),
            total_turnover=float(r.turnover or 0),
            total_debit=float(r.total_debit or 0),
            total_credit=float(r.total_credit or 0),
        )
        for r in rows
        if (r.category or "").strip()
    ]
    return CategorySummaryResponse(data=data)


# -------------------------------------------------------------------------
# 5. Cash withdrawals / deposits
# -------------------------------------------------------------------------

@router.get("/cash-top", response_model=TopExpensesResponse)
async def cash_top(
    type: str = Query("withdrawal", description="withdrawal | deposit"),
    limit: int = Query(10, ge=1, le=100),
    date: Optional[str] = Query(None, description="Exact date DD.MM.YYYY"),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    min_amount: Optional[float] = Query(None),
    max_amount: Optional[float] = Query(None),
    currency: Optional[str] = Query(None),
    sender: Optional[str] = Query(None),
    recipient: Optional[str] = Query(None),
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    if type == "deposit":
        amount_col = Transaction.credit
        group_name = Transaction.recipient_name
        group_iin = Transaction.recipient_iin_bin
        group_acc = Transaction.recipient_account
        purpose_cond = _cash_deposit_condition()
    else:
        amount_col = Transaction.debit
        group_name = Transaction.sender_name
        group_iin = Transaction.sender_iin_bin
        group_acc = Transaction.sender_account
        purpose_cond = _cash_withdrawal_condition()

    display_name = _display_name_expr(group_name, group_acc)
    valid_display = and_(display_name.isnot(None), display_name != "")
    base_cond = _project_where(
        ctx.project.project_id,
        amount_col > 0,
        purpose_cond,
        valid_display,
        *_shared_filter_conditions(
            date=date,
            date_from=None,
            date_to=None,
            category=category,
            search=search,
            min_amount=min_amount,
            max_amount=max_amount,
            currency=currency,
            sender=sender,
            recipient=recipient,
        ),
    )

    q = (
        select(
            display_name.label("cp_name"),
            group_iin.label("cp_iin"),
            group_acc.label("cp_acc"),
            func.sum(amount_col).label("amount"),
            func.count(Transaction.id).label("tx_count"),
            func.max(_effective_dt_expr()).label("last_tx_date"),
        )
        .where(base_cond)
        .group_by(display_name, group_iin, group_acc)
        .order_by(func.sum(amount_col).desc())
        .limit(limit)
    )

    rows = (await db.execute(q)).all()
    total_q = select(func.coalesce(func.sum(amount_col), 0)).where(base_cond)
    grand_total = float((await db.execute(total_q)).scalar() or 0)

    merged: dict[str, dict] = {}
    for r in rows:
        cp_name = _fix_mojibake(r.cp_name or "")
        cp_iin = _normalize_iin(r.cp_iin or "")
        cp_acc = _normalize_account(r.cp_acc or "")
        key = _counterparty_key(cp_iin, cp_acc, cp_name)
        item = merged.get(key)
        if not item:
            merged[key] = {
                "name": cp_name,
                "iin": cp_iin,
                "acc": "" if (cp_iin and cp_iin not in {"0", "000000000000"}) else cp_acc,
                "amount": float(r.amount or 0),
                "tx_count": int(r.tx_count or 0),
                "last_tx_date": r.last_tx_date,
            }
            continue
        item["name"] = _pick_better_display_name(item["name"], cp_name)
        item["amount"] += float(r.amount or 0)
        item["tx_count"] += int(r.tx_count or 0)
        if r.last_tx_date and (not item["last_tx_date"] or r.last_tx_date > item["last_tx_date"]):
            item["last_tx_date"] = r.last_tx_date

    data = [
        TopExpenseItem(
            counterparty=CounterpartyOut(
                name=v["name"], iin_bin=v["iin"], account=v["acc"],
            ),
            amount=float(v["amount"]),
            transaction_count=int(v["tx_count"]),
            percentage=round(float(v["amount"]) / grand_total * 100, 2) if grand_total else 0,
            last_transaction_date=(
                v["last_tx_date"].strftime("%d.%m.%Y")
                if v.get("last_tx_date")
                else None
            ),
        )
        for v in sorted(merged.values(), key=lambda x: x["amount"], reverse=True)[:limit]
    ]

    out_type = "cash_deposit" if type == "deposit" else "cash_withdrawal"
    return TopExpensesResponse(type=out_type, total=grand_total, data=data)


@router.get("/cash-transactions", response_model=CashTransactionsResponse)
async def cash_transactions(
    type: str = Query("withdrawal", description="withdrawal | deposit"),
    iin_bin: str = Query(..., description="Counterparty IIN/BIN"),
    account: Optional[str] = Query(None, description="Counterparty account"),
    limit: int = Query(100, ge=1, le=500),
    date: Optional[str] = Query(None, description="Exact date DD.MM.YYYY"),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    min_amount: Optional[float] = Query(None),
    max_amount: Optional[float] = Query(None),
    currency: Optional[str] = Query(None),
    sender: Optional[str] = Query(None),
    recipient: Optional[str] = Query(None),
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    if type == "deposit":
        amount_col = Transaction.credit
        cp_name_col = Transaction.recipient_name
        cp_iin_col = Transaction.recipient_iin_bin
        cp_acc_col = Transaction.recipient_account
        cp_cond = (cp_iin_col == iin_bin)
        if account:
            cp_cond = and_(cp_cond, cp_acc_col == account)
        purpose_cond = _cash_deposit_condition()
    else:
        amount_col = Transaction.debit
        cp_name_col = Transaction.sender_name
        cp_iin_col = Transaction.sender_iin_bin
        cp_acc_col = Transaction.sender_account
        cp_cond = (cp_iin_col == iin_bin)
        if account:
            cp_cond = and_(cp_cond, cp_acc_col == account)
        purpose_cond = _cash_withdrawal_condition()

    where = _project_where(
        ctx.project.project_id,
        amount_col > 0,
        purpose_cond,
        cp_cond,
        *_shared_filter_conditions(
            date=date,
            date_from=None,
            date_to=None,
            category=category,
            search=search,
            min_amount=min_amount,
            max_amount=max_amount,
            currency=currency,
            sender=sender,
            recipient=recipient,
        ),
    )

    total_q = select(func.count(Transaction.id)).where(where)
    total = int((await db.execute(total_q)).scalar() or 0)

    rows_q = (
        select(Transaction)
        .where(where)
        .order_by(_effective_dt_expr().desc(), Transaction.id.desc())
        .limit(limit)
    )
    rows = (await db.execute(rows_q)).scalars().all()

    data = [
        CashTransactionItem(
            id=str(t.id),
            date=_format_tx_dt(t),
            sender_name=_resolve_display_name(t.sender_name, t.sender_account) or "?",
            recipient_name=_resolve_display_name(t.recipient_name, t.recipient_account) or "?",
            purpose=_fix_mojibake(t.purpose or ""),
            currency=t.currency or "",
            debit=float(t.debit or 0),
            credit=float(t.credit or 0),
            amount_tenge=float(t.amount_tenge or 0),
        )
        for t in rows
    ]

    cp_name_q = select(cp_name_col, cp_acc_col).where(
        _project_where(ctx.project.project_id, cp_iin_col == iin_bin)
    ).limit(1)
    cp_name_row = (await db.execute(cp_name_q)).first()
    cp_name = _resolve_display_name(
        cp_name_row[0] if cp_name_row else None,
        cp_name_row[1] if cp_name_row else account,
    ) or "?"
    cp_account = account or ""

    return CashTransactionsResponse(
        type="cash_deposit" if type == "deposit" else "cash_withdrawal",
        counterparty=CounterpartyOut(name=cp_name, iin_bin=iin_bin, account=cp_account),
        total=total,
        data=data,
    )


@router.get("/counterparty-transactions", response_model=CounterpartyTransactionsResponse)
async def counterparty_transactions(
    iin_bin: str = Query(..., description="Counterparty IIN/BIN"),
    account: Optional[str] = Query(None, description="Counterparty account"),
    limit: int = Query(200, ge=1, le=500),
    date: Optional[str] = Query(None, description="Exact date DD.MM.YYYY"),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    min_amount: Optional[float] = Query(None),
    max_amount: Optional[float] = Query(None),
    currency: Optional[str] = Query(None),
    sender: Optional[str] = Query(None),
    recipient: Optional[str] = Query(None),
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    identity_cond = or_(
        Transaction.sender_iin_bin == iin_bin,
        Transaction.recipient_iin_bin == iin_bin,
    )
    if account:
        identity_cond = and_(
            identity_cond,
            or_(
                Transaction.sender_account == account,
                Transaction.recipient_account == account,
            ),
        )
    where = _project_where(
        ctx.project.project_id,
        identity_cond,
        *_shared_filter_conditions(
            date=date,
            date_from=None,
            date_to=None,
            category=category,
            search=search,
            min_amount=min_amount,
            max_amount=max_amount,
            currency=currency,
            sender=sender,
            recipient=recipient,
        ),
    )

    total_q = select(func.count(Transaction.id)).where(where)
    total = int((await db.execute(total_q)).scalar() or 0)

    rows_q = (
        select(Transaction)
        .where(where)
        .order_by(_effective_dt_expr().desc(), Transaction.id.desc())
        .limit(limit)
    )
    rows = (await db.execute(rows_q)).scalars().all()

    data = [
        CashTransactionItem(
            id=str(t.id),
            date=_format_tx_dt(t),
            sender_name=_resolve_display_name(t.sender_name, t.sender_account) or "—",
            recipient_name=_resolve_display_name(t.recipient_name, t.recipient_account) or "—",
            purpose=_fix_mojibake(t.purpose or ""),
            currency=t.currency or "",
            debit=float(t.debit or 0),
            credit=float(t.credit or 0),
            amount_tenge=float(t.amount_tenge or 0),
        )
        for t in rows
    ]

    cp_name_q = select(Transaction.sender_name, Transaction.sender_account).where(
        _project_where(ctx.project.project_id, Transaction.sender_iin_bin == iin_bin)
    ).limit(1)
    cp_name_row = (await db.execute(cp_name_q)).first()
    cp_name = _resolve_display_name(
        cp_name_row[0] if cp_name_row else None,
        cp_name_row[1] if cp_name_row else account,
    )
    if not cp_name:
        cp_name_q = select(Transaction.recipient_name, Transaction.recipient_account).where(
            _project_where(ctx.project.project_id, Transaction.recipient_iin_bin == iin_bin)
        ).limit(1)
        cp_name_row = (await db.execute(cp_name_q)).first()
        cp_name = _resolve_display_name(
            cp_name_row[0] if cp_name_row else None,
            cp_name_row[1] if cp_name_row else account,
        )

    return CounterpartyTransactionsResponse(
        counterparty=CounterpartyOut(name=cp_name or "—", iin_bin=iin_bin, account=account or ""),
        total=total,
        data=data,
    )


@router.get("/edge-transactions", response_model=EdgeTransactionsResponse)
async def edge_transactions(
    source_iin_bin: str = Query(..., description="Source node IIN/BIN"),
    target_iin_bin: str = Query(..., description="Target node IIN/BIN"),
    limit: int = Query(200, ge=1, le=1000),
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    source_iin_bin = _normalize_iin(source_iin_bin)
    target_iin_bin = _normalize_iin(target_iin_bin)

    if not source_iin_bin or not target_iin_bin:
        raise HTTPException(status_code=422, detail="source_iin_bin and target_iin_bin are required")

    pair_cond = or_(
        and_(
            Transaction.sender_iin_bin == source_iin_bin,
            Transaction.recipient_iin_bin == target_iin_bin,
        ),
        and_(
            Transaction.sender_iin_bin == target_iin_bin,
            Transaction.recipient_iin_bin == source_iin_bin,
        ),
    )
    where = _project_where(ctx.project.project_id, pair_cond)

    total_q = select(func.count(Transaction.id)).where(where)
    total = int((await db.execute(total_q)).scalar() or 0)

    rows_q = (
        select(Transaction)
        .where(where)
        .order_by(_effective_dt_expr().desc(), Transaction.id.desc())
        .limit(limit)
    )
    rows = (await db.execute(rows_q)).scalars().all()

    data = [
        CashTransactionItem(
            id=str(t.id),
            date=_format_tx_dt(t),
            sender_name=_resolve_display_name(t.sender_name, t.sender_account) or "—",
            recipient_name=_resolve_display_name(t.recipient_name, t.recipient_account) or "—",
            purpose=_fix_mojibake(t.purpose or ""),
            currency=t.currency or "",
            debit=float(t.debit or 0),
            credit=float(t.credit or 0),
            amount_tenge=float(t.amount_tenge or 0),
        )
        for t in rows
    ]

    source_name = None
    target_name = None
    for t in rows:
        if source_name is None and _normalize_iin(t.sender_iin_bin or "") == source_iin_bin:
            source_name = _resolve_display_name(t.sender_name, t.sender_account)
        if target_name is None and _normalize_iin(t.recipient_iin_bin or "") == target_iin_bin:
            target_name = _resolve_display_name(t.recipient_name, t.recipient_account)
        if source_name and target_name:
            break

    if not source_name:
        source_row = (
            await db.execute(
                select(Transaction.sender_name, Transaction.sender_account)
                .where(_project_where(ctx.project.project_id, Transaction.sender_iin_bin == source_iin_bin))
                .limit(1)
            )
        ).first()
        source_name = _resolve_display_name(
            source_row[0] if source_row else None,
            source_row[1] if source_row else None,
        ) or source_iin_bin

    if not target_name:
        target_row = (
            await db.execute(
                select(Transaction.recipient_name, Transaction.recipient_account)
                .where(_project_where(ctx.project.project_id, Transaction.recipient_iin_bin == target_iin_bin))
                .limit(1)
            )
        ).first()
        target_name = _resolve_display_name(
            target_row[0] if target_row else None,
            target_row[1] if target_row else None,
        ) or target_iin_bin

    return EdgeTransactionsResponse(
        source=CounterpartyOut(name=_fix_mojibake(source_name), iin_bin=source_iin_bin, account=""),
        target=CounterpartyOut(name=_fix_mojibake(target_name), iin_bin=target_iin_bin, account=""),
        total=total,
        data=data,
    )

def _parse_graph_limit(value: str, *, name: str, min_value: int = 1, max_value: int | None = None) -> int | None:
    raw = str(value or "").strip().lower()
    if raw == "max":
        return None

    try:
        parsed = int(raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=f"{name} must be an integer or 'max'") from exc

    if parsed < min_value:
        raise HTTPException(status_code=422, detail=f"{name} must be >= {min_value}")
    if max_value is not None and parsed > max_value:
        raise HTTPException(status_code=422, detail=f"{name} must be <= {max_value} or 'max'")
    return parsed


@router.get("/counterparty-graph", response_model=CounterpartyGraphResponse)
async def counterparty_graph(
    iin_bin: str = Query(..., description="Counterparty IIN/BIN"),
    depth: str = Query("2", description="Depth level or 'max'"),
    max_neighbors: str = Query("6", description="Max neighbors per node or 'max'"),
    ctx: ProjectContext = Depends(get_current_project_context),
    db: AsyncSession = Depends(get_db),
):
    depth_limit = _parse_graph_limit(depth, name="depth", min_value=1)
    neighbors_limit = _parse_graph_limit(max_neighbors, name="max_neighbors", min_value=1, max_value=20)
    iin_bin = _normalize_iin(iin_bin)
    node_meta: dict[str, dict] = {
        iin_bin: {"label": iin_bin, "level": 0, "total_turnover": 0.0}
    }
    edges_map: dict[tuple[str, str], dict] = {}

    frontier = {iin_bin}
    visited = {iin_bin}
    level = 1

    while frontier and (depth_limit is None or level <= depth_limit):
        next_frontier = set()
        for current in frontier:
            in_q = (
                select(
                    Transaction.sender_iin_bin.label("neighbor_iin"),
                    Transaction.sender_name.label("neighbor_name"),
                    Transaction.sender_account.label("neighbor_account"),
                    func.coalesce(func.sum(Transaction.amount_tenge), 0).label("amount"),
                    func.count(Transaction.id).label("tx_count"),
                )
                .where(_project_where(ctx.project.project_id, Transaction.recipient_iin_bin == current))
                .group_by(Transaction.sender_iin_bin, Transaction.sender_name, Transaction.sender_account)
            )
            out_q = (
                select(
                    Transaction.recipient_iin_bin.label("neighbor_iin"),
                    Transaction.recipient_name.label("neighbor_name"),
                    Transaction.recipient_account.label("neighbor_account"),
                    func.coalesce(func.sum(Transaction.amount_tenge), 0).label("amount"),
                    func.count(Transaction.id).label("tx_count"),
                )
                .where(_project_where(ctx.project.project_id, Transaction.sender_iin_bin == current))
                .group_by(Transaction.recipient_iin_bin, Transaction.recipient_name, Transaction.recipient_account)
            )

            rows = (await db.execute(in_q)).all() + (await db.execute(out_q)).all()
            agg: dict[str, dict] = {}
            for r in rows:
                neighbor_iin = _normalize_iin(r.neighbor_iin or "")
                if not neighbor_iin or neighbor_iin == current:
                    continue
                display_name = _resolve_display_name(r.neighbor_name, r.neighbor_account)
                if not display_name:
                    continue
                if neighbor_iin not in agg:
                    agg[neighbor_iin] = {
                        "name": display_name,
                        "amount": 0.0,
                        "tx_count": 0,
                    }
                agg[neighbor_iin]["amount"] += float(r.amount or 0)
                agg[neighbor_iin]["tx_count"] += int(r.tx_count or 0)

            top_neighbors = sorted(
                agg.items(),
                key=lambda item: item[1]["amount"],
                reverse=True,
            )
            if neighbors_limit is not None:
                top_neighbors = top_neighbors[:neighbors_limit]

            for neighbor_iin, info in top_neighbors:
                edge_key = tuple(sorted((current, neighbor_iin)))
                if edge_key not in edges_map:
                    edges_map[edge_key] = {
                        "source": edge_key[0],
                        "target": edge_key[1],
                        "amount": 0.0,
                        "tx_count": 0,
                    }
                edges_map[edge_key]["amount"] += info["amount"]
                edges_map[edge_key]["tx_count"] += info["tx_count"]

                if neighbor_iin not in node_meta:
                    node_meta[neighbor_iin] = {
                        "label": _fix_mojibake(info["name"]) or neighbor_iin,
                        "level": level,
                        "total_turnover": 0.0,
                    }
                node_meta[neighbor_iin]["total_turnover"] += info["amount"]

                if neighbor_iin not in visited:
                    visited.add(neighbor_iin)
                    next_frontier.add(neighbor_iin)

            node_meta[current]["total_turnover"] += sum(item[1]["amount"] for item in top_neighbors)

        frontier = next_frontier
        level += 1

    center_name_q = select(Transaction.sender_name, Transaction.sender_account).where(
        _project_where(ctx.project.project_id, Transaction.sender_iin_bin == iin_bin)
    ).limit(1)
    center_row = (await db.execute(center_name_q)).first()
    center_name = _resolve_display_name(
        center_row[0] if center_row else None,
        center_row[1] if center_row else None,
    )
    if not center_name:
        center_name_q = select(Transaction.recipient_name, Transaction.recipient_account).where(
            _project_where(ctx.project.project_id, Transaction.recipient_iin_bin == iin_bin)
        ).limit(1)
        center_row = (await db.execute(center_name_q)).first()
        center_name = _resolve_display_name(
            center_row[0] if center_row else None,
            center_row[1] if center_row else None,
        )
    if center_name:
        node_meta[iin_bin]["label"] = _fix_mojibake(center_name)

    raw_nodes = [
        CounterpartyGraphNode(
            id=node_iin,
            label=_fix_mojibake(meta["label"] or "") or node_iin,
            iin_bin=node_iin,
            level=int(meta["level"]),
            total_turnover=float(meta["total_turnover"] or 0),
        )
        for node_iin, meta in node_meta.items()
    ]
    raw_edges = [
        CounterpartyGraphEdge(
            source=meta["source"],
            target=meta["target"],
            amount=float(meta["amount"] or 0),
            tx_count=int(meta["tx_count"] or 0),
        )
        for meta in edges_map.values()
    ]

    # Collapse duplicate nodes with same normalized label and merge their turnovers/edges.
    name_to_canonical: dict[str, str] = {}
    alias: dict[str, str] = {}
    merged_nodes: dict[str, CounterpartyGraphNode] = {}

    for n in sorted(raw_nodes, key=lambda x: (x.level, -x.total_turnover)):
        name_key = _normalize_name(n.label)
        canonical_id = name_to_canonical.get(name_key) if name_key else None
        if canonical_id is None:
            canonical_id = n.id
            if name_key:
                name_to_canonical[name_key] = canonical_id
            merged_nodes[canonical_id] = CounterpartyGraphNode(
                id=canonical_id,
                label=n.label,
                iin_bin=canonical_id,
                level=n.level,
                total_turnover=n.total_turnover,
            )
        else:
            existing = merged_nodes[canonical_id]
            existing.total_turnover += n.total_turnover
            if n.level < existing.level:
                existing.level = n.level
            if len((n.label or "").strip()) > len((existing.label or "").strip()):
                existing.label = n.label
        alias[n.id] = canonical_id

    merged_edges_map: dict[tuple[str, str], CounterpartyGraphEdge] = {}
    for e in raw_edges:
        src = alias.get(e.source, e.source)
        dst = alias.get(e.target, e.target)
        if src == dst:
            continue
        edge_key = tuple(sorted((src, dst)))
        if edge_key not in merged_edges_map:
            merged_edges_map[edge_key] = CounterpartyGraphEdge(
                source=edge_key[0],
                target=edge_key[1],
                amount=0.0,
                tx_count=0,
            )
        merged_edges_map[edge_key].amount += e.amount
        merged_edges_map[edge_key].tx_count += e.tx_count

    nodes = list(merged_nodes.values())
    edges = list(merged_edges_map.values())

    return CounterpartyGraphResponse(
        center_iin_bin=iin_bin,
        nodes=nodes,
        edges=edges,
    )
