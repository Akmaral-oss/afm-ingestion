from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable


@dataclass(frozen=True)
class ImportFraudIndicator:
    label: str
    value: str


@dataclass(frozen=True)
class ImportFraudCounterparty:
    role: str
    name: str
    identifier: str
    transaction_count: int
    turnover: str
    articles: tuple[str, ...] = ()
    graph_iin_bin: str = ""


@dataclass(frozen=True)
class ImportFraudTransaction:
    tx_id: str
    happened_at: str
    direction: str
    amount: str
    counterparty: str
    purpose: str


@dataclass(frozen=True)
class ImportFraudWarning:
    code: str
    title: str
    severity: str
    summary: str
    articles: tuple[str, ...]
    indicators: tuple[ImportFraudIndicator, ...]
    counterparties: tuple[ImportFraudCounterparty, ...]
    sample_transactions: tuple[ImportFraudTransaction, ...]


@dataclass(frozen=True)
class ImportedTransactionSample:
    tx_id: str
    operation_date: date | None
    operation_ts: datetime | None
    amount_kzt: float
    credit: float
    debit: float
    direction: str
    purpose_text: str
    operation_type_raw: str
    transaction_category: str
    payer_name: str
    payer_iin_bin: str
    payer_account: str
    receiver_name: str
    receiver_iin_bin: str
    receiver_account: str


def detect_import_fraud_warnings(rows: Iterable[ImportedTransactionSample]) -> list[ImportFraudWarning]:
    txs = list(rows)
    if len(txs) < 3:
        return []

    credit_txs = [tx for tx in txs if _credit_amount(tx) > 0]
    debit_txs = [tx for tx in txs if _debit_amount(tx) > 0]
    if not credit_txs and not debit_txs:
        return []

    total_credit = sum(_credit_amount(tx) for tx in credit_txs)
    total_debit = sum(_debit_amount(tx) for tx in debit_txs)
    if total_credit <= 0 and total_debit <= 0:
        return []

    distinct_credit_senders = _distinct_counterparties(credit_txs, payer=True)
    distinct_debit_receivers = _distinct_counterparties(debit_txs, payer=False)
    cash_out_txs = [tx for tx in debit_txs if _is_cash_out(tx)]
    cash_out_total = sum(_debit_amount(tx) for tx in cash_out_txs)
    cash_out_count = len(cash_out_txs)
    same_day_turnover_days = _same_day_turnover_days(credit_txs, debit_txs)
    active_days = {_tx_day(tx) for tx in txs if _tx_day(tx) is not None}
    outgoing_ratio = total_debit / total_credit if total_credit > 0 else 0.0
    cash_out_ratio = cash_out_total / total_debit if total_debit > 0 else 0.0
    retention_ratio = abs(total_credit - total_debit) / total_credit if total_credit > 0 else 1.0
    same_day_ratio = len(same_day_turnover_days) / len(active_days) if active_days else 0.0

    warnings: list[ImportFraudWarning] = []

    if (
        total_credit >= 300_000
        and len(credit_txs) >= 5
        and len(distinct_credit_senders) >= 4
        and total_debit >= 200_000
        and (len(distinct_debit_receivers) >= 3 or cash_out_total >= 150_000)
        and outgoing_ratio >= 0.78
        and retention_ratio <= 0.30
        and same_day_ratio >= 0.30
    ):
        relevant = _top_by_amount(credit_txs, _credit_amount, 5) + _top_by_amount(debit_txs, _debit_amount, 5)
        warnings.append(
            ImportFraudWarning(
                code="dropper_transit",
                title="Вероятные признаки дропперского транзита",
                severity="high",
                summary=(
                    "Счет в пределах загруженной выписки похож на транзитный: много входящих "
                    "поступлений от разных контрагентов и быстрый вывод средств дальше или в наличные."
                ),
                articles=("ст. 232-1 УК РК", "ст. 190 УК РК", "ст. 218 УК РК"),
                indicators=(
                    ImportFraudIndicator("Входящих операций", str(len(credit_txs))),
                    ImportFraudIndicator("Разных отправителей", str(len(distinct_credit_senders))),
                    ImportFraudIndicator("Исходящий оборот / входящий оборот", _format_percent(outgoing_ratio)),
                    ImportFraudIndicator("Дни с быстрым оборотом", f"{len(same_day_turnover_days)} из {max(len(active_days), 1)}"),
                    ImportFraudIndicator("Вывод в наличные", _format_money(cash_out_total)),
                ),
                counterparties=tuple(
                    _top_counterparties(credit_txs, payer=True, role="Отправитель", limit=4)
                    + _top_counterparties(debit_txs, payer=False, role="Получатель", limit=4)
                ),
                sample_transactions=tuple(_sample_transactions(relevant, limit=8)),
            )
        )

    if (
        total_credit >= 250_000
        and cash_out_total >= 150_000
        and cash_out_count >= 2
        and cash_out_ratio >= 0.35
    ):
        supporting = _top_by_amount(cash_out_txs, _debit_amount, 6) + _top_by_amount(credit_txs, _credit_amount, 3)
        warnings.append(
            ImportFraudWarning(
                code="cash_out",
                title="Вероятные признаки быстрого вывода в наличные",
                severity="medium",
                summary=(
                    "После поступлений значимая часть средств выводится через операции, "
                    "похожие на снятие наличных или cash-out."
                ),
                articles=("ст. 232-1 УК РК", "ст. 218 УК РК"),
                indicators=(
                    ImportFraudIndicator("Сумма cash-out", _format_money(cash_out_total)),
                    ImportFraudIndicator("Доля cash-out в исходящих", _format_percent(cash_out_ratio)),
                    ImportFraudIndicator("Cash-out операций", str(cash_out_count)),
                ),
                counterparties=tuple(_top_counterparties(cash_out_txs, payer=False, role="Получатель", limit=5)),
                sample_transactions=tuple(_sample_transactions(supporting, limit=8)),
            )
        )

    small_credit_txs = [tx for tx in credit_txs if 0 < _credit_amount(tx) <= 100_000]
    repeated_credit_amounts = [
        amount
        for amount, count in Counter(round(_credit_amount(tx), 2) for tx in credit_txs).items()
        if count >= 3
    ]
    small_credit_share = len(small_credit_txs) / len(credit_txs) if credit_txs else 0.0

    if (
        total_credit >= 250_000
        and len(small_credit_txs) >= 6
        and len(distinct_credit_senders) >= 4
        and small_credit_share >= 0.60
    ):
        relevant_small = _top_by_amount(small_credit_txs, _credit_amount, 8)
        warnings.append(
            ImportFraudWarning(
                code="structuring",
                title="Вероятные признаки дробления поступлений",
                severity="medium",
                summary=(
                    "Во входящем потоке много однотипных небольших поступлений от разных лиц, "
                    "что может указывать на дробление или сбор средств через транзитный счет."
                ),
                articles=("ст. 190 УК РК", "ст. 218 УК РК"),
                indicators=(
                    ImportFraudIndicator("Небольших поступлений", str(len(small_credit_txs))),
                    ImportFraudIndicator("Доля небольших поступлений", _format_percent(small_credit_share)),
                    ImportFraudIndicator("Разных отправителей", str(len(distinct_credit_senders))),
                    ImportFraudIndicator("Повторяющихся сумм", str(len(repeated_credit_amounts))),
                ),
                counterparties=tuple(_top_counterparties(small_credit_txs, payer=True, role="Отправитель", limit=6)),
                sample_transactions=tuple(_sample_transactions(relevant_small, limit=8)),
            )
        )

    return [_enrich_warning_counterparties(warning) for warning in warnings]


def _credit_amount(tx: ImportedTransactionSample) -> float:
    if tx.credit > 0:
        return float(tx.credit)
    if str(tx.direction or "").strip().lower() == "credit" and tx.amount_kzt > 0:
        return float(tx.amount_kzt)
    return 0.0


def _debit_amount(tx: ImportedTransactionSample) -> float:
    if tx.debit > 0:
        return float(tx.debit)
    if str(tx.direction or "").strip().lower() == "debit" and tx.amount_kzt > 0:
        return float(tx.amount_kzt)
    return 0.0


def _tx_day(tx: ImportedTransactionSample) -> date | None:
    if tx.operation_date is not None:
        return tx.operation_date
    if tx.operation_ts is not None:
        return tx.operation_ts.date()
    return None


def _tx_dt_sort_key(tx: ImportedTransactionSample) -> tuple[date, datetime]:
    tx_day = _tx_day(tx) or date.min
    tx_dt = tx.operation_ts or datetime.combine(tx_day, datetime.min.time())
    return tx_day, tx_dt


def _same_day_turnover_days(
    credits: list[ImportedTransactionSample],
    debits: list[ImportedTransactionSample],
) -> set[date]:
    credit_days = {_tx_day(tx) for tx in credits if _tx_day(tx) is not None}
    debit_days = {_tx_day(tx) for tx in debits if _tx_day(tx) is not None}
    return credit_days & debit_days


def _distinct_counterparties(
    txs: list[ImportedTransactionSample],
    *,
    payer: bool,
) -> set[str]:
    values: set[str] = set()
    for tx in txs:
        key = _counterparty_key_for_tx(tx, payer=payer)
        if key:
            values.add(key)
    return values


def _counterparty_key_for_tx(tx: ImportedTransactionSample, *, payer: bool) -> str:
    if payer:
        return _counterparty_key(tx.payer_iin_bin, tx.payer_account, tx.payer_name)
    return _counterparty_key(tx.receiver_iin_bin, tx.receiver_account, tx.receiver_name)


def _counterparty_key(iin_bin: str, account: str, name: str) -> str:
    iin = "".join(ch for ch in str(iin_bin or "") if ch.isdigit())
    if iin:
        return f"iin:{iin}"
    account_value = str(account or "").strip().upper()
    if account_value:
        return f"acc:{account_value}"
    normalized_name = " ".join(str(name or "").strip().lower().split())
    return f"name:{normalized_name}" if normalized_name else ""


def _counterparty_label(tx: ImportedTransactionSample, *, payer: bool) -> str:
    if payer:
        primary_name = str(tx.payer_name or "").strip()
        identifier = "".join(ch for ch in str(tx.payer_iin_bin or "") if ch.isdigit()) or str(tx.payer_account or "").strip()
    else:
        primary_name = str(tx.receiver_name or "").strip()
        identifier = "".join(ch for ch in str(tx.receiver_iin_bin or "") if ch.isdigit()) or str(tx.receiver_account or "").strip()
    return primary_name or identifier or "Не указан"


def _counterparty_identifier(tx: ImportedTransactionSample, *, payer: bool) -> str:
    if payer:
        identifier = "".join(ch for ch in str(tx.payer_iin_bin or "") if ch.isdigit()) or str(tx.payer_account or "").strip()
    else:
        identifier = "".join(ch for ch in str(tx.receiver_iin_bin or "") if ch.isdigit()) or str(tx.receiver_account or "").strip()
    return identifier or "—"


def _counterparty_graph_iin(tx: ImportedTransactionSample, *, payer: bool) -> str:
    raw_value = tx.payer_iin_bin if payer else tx.receiver_iin_bin
    return "".join(ch for ch in str(raw_value or "") if ch.isdigit())


def _top_counterparties(
    txs: list[ImportedTransactionSample],
    *,
    payer: bool,
    role: str,
    limit: int,
) -> list[ImportFraudCounterparty]:
    stats: dict[str, dict] = defaultdict(
        lambda: {"count": 0, "amount": 0.0, "name": "", "identifier": "", "graph_iin_bin": ""}
    )
    for tx in txs:
        key = _counterparty_key_for_tx(tx, payer=payer)
        if not key:
            continue
        amount = _credit_amount(tx) if payer else _debit_amount(tx)
        stats[key]["count"] += 1
        stats[key]["amount"] += amount
        stats[key]["name"] = stats[key]["name"] or _counterparty_label(tx, payer=payer)
        stats[key]["identifier"] = stats[key]["identifier"] or _counterparty_identifier(tx, payer=payer)
        stats[key]["graph_iin_bin"] = stats[key]["graph_iin_bin"] or _counterparty_graph_iin(tx, payer=payer)

    ordered = sorted(
        stats.values(),
        key=lambda item: (item["amount"], item["count"]),
        reverse=True,
    )[:limit]
    return [
        ImportFraudCounterparty(
            role=role,
            name=item["name"] or "Не указан",
            identifier=item["identifier"] or "—",
            transaction_count=int(item["count"]),
            turnover=_format_money(float(item["amount"])),
            graph_iin_bin=item["graph_iin_bin"] or "",
        )
        for item in ordered
    ]


def _enrich_warning_counterparties(warning: ImportFraudWarning) -> ImportFraudWarning:
    return ImportFraudWarning(
        code=warning.code,
        title=warning.title,
        severity=warning.severity,
        summary=warning.summary,
        articles=warning.articles,
        indicators=warning.indicators,
        counterparties=tuple(
            ImportFraudCounterparty(
                role=counterparty.role,
                name=counterparty.name,
                identifier=counterparty.identifier,
                transaction_count=counterparty.transaction_count,
                turnover=counterparty.turnover,
                articles=warning.articles,
                graph_iin_bin=counterparty.graph_iin_bin,
            )
            for counterparty in warning.counterparties
        ),
        sample_transactions=warning.sample_transactions,
    )


def _sample_transactions(
    txs: list[ImportedTransactionSample],
    *,
    limit: int,
) -> list[ImportFraudTransaction]:
    unique: dict[str, ImportedTransactionSample] = {}
    for tx in sorted(txs, key=lambda item: (_amount_abs(item), _tx_dt_sort_key(item)), reverse=True):
        if tx.tx_id not in unique:
            unique[tx.tx_id] = tx
        if len(unique) >= limit:
            break

    ordered = sorted(unique.values(), key=_tx_dt_sort_key, reverse=True)
    return [
        ImportFraudTransaction(
            tx_id=tx.tx_id,
            happened_at=_format_tx_datetime(tx),
            direction="Входящая" if _credit_amount(tx) > 0 else "Исходящая",
            amount=_format_money(_credit_amount(tx) or _debit_amount(tx) or tx.amount_kzt),
            counterparty=_counterparty_label(tx, payer=_credit_amount(tx) > 0),
            purpose=_truncate_text((tx.purpose_text or tx.operation_type_raw or tx.transaction_category or "").strip(), 120),
        )
        for tx in ordered
    ]


def _top_by_amount(
    txs: list[ImportedTransactionSample],
    selector,
    limit: int,
) -> list[ImportedTransactionSample]:
    return sorted(txs, key=lambda tx: selector(tx), reverse=True)[:limit]


def _amount_abs(tx: ImportedTransactionSample) -> float:
    return max(_credit_amount(tx), _debit_amount(tx), float(tx.amount_kzt or 0))


def _format_tx_datetime(tx: ImportedTransactionSample) -> str:
    if tx.operation_ts is not None:
        return tx.operation_ts.strftime("%d.%m.%Y %H:%M")
    if tx.operation_date is not None:
        return tx.operation_date.strftime("%d.%m.%Y")
    return "Без даты"


def _truncate_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value or "—"
    return f"{value[: limit - 1].rstrip()}…"


def _is_cash_out(tx: ImportedTransactionSample) -> bool:
    if _debit_amount(tx) <= 0:
        return False

    haystack = " | ".join(
        [
            str(tx.purpose_text or "").lower(),
            str(tx.operation_type_raw or "").lower(),
            str(tx.transaction_category or "").lower(),
        ]
    )
    keywords = (
        "снятие",
        "налич",
        "cash",
        "atm",
        "банкомат",
        "cash-out",
        "cash out",
    )
    return any(keyword in haystack for keyword in keywords)


def _format_money(value: float) -> str:
    return f"{value:,.0f} KZT".replace(",", " ")


def _format_percent(value: float) -> str:
    return f"{value * 100:.0f}%"
