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
    sender_name: str = ""
    sender_iin_bin: str = ""
    sender_account: str = ""
    recipient_name: str = ""
    recipient_iin_bin: str = ""
    recipient_account: str = ""


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

    repeated_amount_groups = _repeated_amount_groups(txs)
    repeated_amount_txs = [tx for group in repeated_amount_groups for tx in group["txs"]]
    repeated_amount_share = len({tx.tx_id for tx in repeated_amount_txs}) / len(txs) if txs else 0.0
    if repeated_amount_groups and repeated_amount_share >= 0.25:
        top_amount_group = repeated_amount_groups[0]
        warnings.append(
            ImportFraudWarning(
                code="repeated_amount_patterns",
                title="\u041f\u043e\u0432\u0442\u043e\u0440\u044f\u044e\u0449\u0438\u0435\u0441\u044f \u0441\u0443\u043c\u043c\u044b \u0438 \u0448\u0430\u0431\u043b\u043e\u043d\u043d\u044b\u0435 \u043f\u0435\u0440\u0435\u0432\u043e\u0434\u044b",
                severity="medium",
                summary=(
                    "\u0412 \u0432\u044b\u043f\u0438\u0441\u043a\u0435 \u043c\u043d\u043e\u0433\u043e \u043e\u043f\u0435\u0440\u0430\u0446\u0438\u0439 \u0441 \u043e\u0434\u0438\u043d\u0430\u043a\u043e\u0432\u044b\u043c\u0438 \u0438\u043b\u0438 \u043e\u0447\u0435\u043d\u044c \u0431\u043b\u0438\u0437\u043a\u0438\u043c\u0438 \u0441\u0443\u043c\u043c\u0430\u043c\u0438. "
                    "\u042d\u0442\u043e \u043f\u043e\u0445\u043e\u0436\u0435 \u043d\u0430 \u0441\u0442\u0440\u0443\u043a\u0442\u0443\u0440\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u0443\u044e \u0441\u0445\u0435\u043c\u0443 \u0441 \u043f\u043e\u0432\u0442\u043e\u0440\u044f\u044e\u0449\u0438\u043c\u0438\u0441\u044f \u043d\u043e\u043c\u0438\u043d\u0430\u043b\u0430\u043c\u0438."
                ),
                articles=("\u0441\u0442. 190 \u0423\u041a \u0420\u041a", "\u0441\u0442. 218 \u0423\u041a \u0420\u041a"),
                indicators=(
                    ImportFraudIndicator("\u041f\u043e\u0432\u0442\u043e\u0440\u044f\u044e\u0449\u0438\u0445\u0441\u044f \u043d\u043e\u043c\u0438\u043d\u0430\u043b\u043e\u0432", str(len(repeated_amount_groups))),
                    ImportFraudIndicator("\u0421\u0430\u043c\u044b\u0439 \u0447\u0430\u0441\u0442\u044b\u0439 \u043d\u043e\u043c\u0438\u043d\u0430\u043b", _format_money(top_amount_group["amount"])),
                    ImportFraudIndicator("\u041f\u043e\u0432\u0442\u043e\u0440\u043e\u0432 \u043f\u043e \u043d\u043e\u043c\u0438\u043d\u0430\u043b\u0443", str(top_amount_group["count"])),
                    ImportFraudIndicator("\u0414\u043e\u043b\u044f \u0448\u0430\u0431\u043b\u043e\u043d\u043d\u044b\u0445 \u043e\u043f\u0435\u0440\u0430\u0446\u0438\u0439", _format_percent(repeated_amount_share)),
                ),
                counterparties=tuple(_top_counterparties(repeated_amount_txs, payer=_credit_amount(repeated_amount_txs[0]) > 0, role="\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442", limit=6)),
                sample_transactions=tuple(_sample_transactions(_top_by_amount(repeated_amount_txs, _amount_abs, 8), limit=8)),
            )
        )

    high_activity_days = _high_activity_days(txs)
    if high_activity_days:
        day_stats = high_activity_days[0]
        warnings.append(
            ImportFraudWarning(
                code="high_activity_spike",
                title="\u041d\u0435\u043e\u0431\u044b\u0447\u043d\u043e \u0432\u044b\u0441\u043e\u043a\u0430\u044f \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0441\u0442\u044c \u0437\u0430 \u043a\u043e\u0440\u043e\u0442\u043a\u0438\u0439 \u0441\u0440\u043e\u043a",
                severity="medium",
                summary=(
                    "\u0412 \u043e\u0434\u0438\u043d \u0438\u0437 \u0434\u043d\u0435\u0439 \u0432 \u0432\u044b\u043f\u0438\u0441\u043a\u0435 \u0437\u0430\u0444\u0438\u043a\u0441\u0438\u0440\u043e\u0432\u0430\u043d \u0441\u043a\u0430\u0447\u043e\u043a \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0441\u0442\u0438: \u043c\u043d\u043e\u0433\u043e \u043e\u043f\u0435\u0440\u0430\u0446\u0438\u0439 \u0438 \u0432\u044b\u0441\u043e\u043a\u0438\u0439 \u043e\u0431\u043e\u0440\u043e\u0442 "
                    "\u0437\u0430 \u043e\u0447\u0435\u043d\u044c \u043a\u043e\u0440\u043e\u0442\u043a\u043e\u0435 \u0432\u0440\u0435\u043c\u044f."
                ),
                articles=("\u0441\u0442. 232-1 \u0423\u041a \u0420\u041a", "\u0441\u0442. 218 \u0423\u041a \u0420\u041a"),
                indicators=(
                    ImportFraudIndicator("\u0414\u0430\u0442\u0430 \u043f\u0438\u043a\u0430", day_stats["day"].strftime("%d.%m.%Y")),
                    ImportFraudIndicator("\u041e\u043f\u0435\u0440\u0430\u0446\u0438\u0439 \u0437\u0430 \u0434\u0435\u043d\u044c", str(day_stats["count"])),
                    ImportFraudIndicator("\u041e\u0431\u043e\u0440\u043e\u0442 \u0437\u0430 \u0434\u0435\u043d\u044c", _format_money(day_stats["turnover"])),
                    ImportFraudIndicator("\u0420\u0430\u0437\u043d\u044b\u0445 \u043a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442\u043e\u0432", str(day_stats["distinct_counterparties"])),
                ),
                counterparties=tuple(_top_counterparties(day_stats["txs"], payer=_credit_amount(day_stats["txs"][0]) > 0, role="\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442", limit=6)),
                sample_transactions=tuple(_sample_transactions(day_stats["txs"], limit=8)),
            )
        )

    repeated_counterparties = _repeated_counterparty_relationships(txs)
    if repeated_counterparties:
        top_cp = repeated_counterparties[0]
        warnings.append(
            ImportFraudWarning(
                code="repeated_counterparty_flows",
                title="\u0427\u0430\u0441\u0442\u044b\u0435 \u043f\u0435\u0440\u0435\u0432\u043e\u0434\u044b \u043c\u0435\u0436\u0434\u0443 \u043e\u0434\u043d\u0438\u043c\u0438 \u0438 \u0442\u0435\u043c\u0438 \u0436\u0435 \u043b\u0438\u0446\u0430\u043c\u0438",
                severity="medium",
                summary=(
                    "\u041e\u0434\u0438\u043d \u0438 \u0442\u043e\u0442 \u0436\u0435 \u043a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442 \u043f\u043e\u0432\u0442\u043e\u0440\u044f\u0435\u0442\u0441\u044f \u0432 \u0431\u043e\u043b\u044c\u0448\u043e\u043c \u0447\u0438\u0441\u043b\u0435 \u043e\u043f\u0435\u0440\u0430\u0446\u0438\u0439. "
                    "\u0420\u0435\u0433\u0443\u043b\u044f\u0440\u043d\u044b\u0439 \u043e\u0431\u043c\u0435\u043d \u0441\u0440\u0435\u0434\u0441\u0442\u0432\u0430\u043c\u0438 \u0431\u0435\u0437 \u044f\u0441\u043d\u043e\u0439 \u044d\u043a\u043e\u043d\u043e\u043c\u0438\u0447\u0435\u0441\u043a\u043e\u0439 \u043b\u043e\u0433\u0438\u043a\u0438 \u043c\u043e\u0436\u0435\u0442 \u0443\u043a\u0430\u0437\u044b\u0432\u0430\u0442\u044c \u043d\u0430 \u043e\u0431\u043d\u0430\u043b\u0438\u0447\u0438\u0432\u0430\u043d\u0438\u0435 \u0438\u043b\u0438 \u0442\u0440\u0430\u043d\u0437\u0438\u0442."
                ),
                articles=("\u0441\u0442. 190 \u0423\u041a \u0420\u041a", "\u0441\u0442. 232-1 \u0423\u041a \u0420\u041a"),
                indicators=(
                    ImportFraudIndicator("\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442", top_cp["name"]),
                    ImportFraudIndicator("\u041e\u043f\u0435\u0440\u0430\u0446\u0438\u0439 \u0441 \u043d\u0438\u043c", str(top_cp["count"])),
                    ImportFraudIndicator("\u041e\u0431\u043e\u0440\u043e\u0442", _format_money(top_cp["turnover"])),
                    ImportFraudIndicator("\u0410\u043a\u0442\u0438\u0432\u043d\u044b\u0445 \u0434\u043d\u0435\u0439", str(top_cp["days"])),
                ),
                counterparties=tuple(_relationship_counterparties(repeated_counterparties[:6])),
                sample_transactions=tuple(_sample_transactions(top_cp["txs"], limit=8)),
            )
        )

    fan_in_targets = _fan_in_targets(credit_txs)
    if fan_in_targets:
        top_target = fan_in_targets[0]
        warnings.append(
            ImportFraudWarning(
                code="fan_in_drop_account",
                title="\u041c\u043d\u043e\u0433\u043e \u043f\u043e\u0441\u0442\u0443\u043f\u043b\u0435\u043d\u0438\u0439 \u043e\u0442 \u0440\u0430\u0437\u043d\u044b\u0445 \u043b\u0438\u0446 \u043d\u0430 \u043e\u0434\u0438\u043d \u0441\u0447\u0451\u0442",
                severity="high",
                summary=(
                    "\u041e\u0434\u0438\u043d \u0441\u0447\u0451\u0442 \u0430\u043a\u043a\u0443\u043c\u0443\u043b\u0438\u0440\u0443\u0435\u0442 \u043f\u043e\u0441\u0442\u0443\u043f\u043b\u0435\u043d\u0438\u044f \u043e\u0442 \u043c\u043d\u043e\u0436\u0435\u0441\u0442\u0432\u0430 \u0440\u0430\u0437\u043d\u044b\u0445 \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u0435\u043b\u0435\u0439. "
                    "\u042d\u0442\u043e \u043f\u043e\u0445\u043e\u0436\u0435 \u043d\u0430 \u043f\u043e\u0432\u0435\u0434\u0435\u043d\u0438\u0435 \u201c\u0434\u0440\u043e\u043f-\u0441\u0447\u0451\u0442\u0430\u201d \u0438\u043b\u0438 \u0442\u0440\u0430\u043d\u0437\u0438\u0442\u043d\u043e\u0439 \u0442\u043e\u0447\u043a\u0438 \u0441\u0431\u043e\u0440\u0430 \u0434\u0435\u043d\u0435\u0433."
                ),
                articles=("\u0441\u0442. 232-1 \u0423\u041a \u0420\u041a", "\u0441\u0442. 218 \u0423\u041a \u0420\u041a", "\u0441\u0442. 190 \u0423\u041a \u0420\u041a"),
                indicators=(
                    ImportFraudIndicator("\u041f\u043e\u0441\u0442\u0443\u043f\u043b\u0435\u043d\u0438\u0439", str(top_target["count"])),
                    ImportFraudIndicator("\u0420\u0430\u0437\u043d\u044b\u0445 \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u0435\u043b\u0435\u0439", str(top_target["distinct_senders"])),
                    ImportFraudIndicator("\u0421\u0443\u043c\u043c\u0430 \u0441\u0431\u043e\u0440\u0430", _format_money(top_target["turnover"])),
                    ImportFraudIndicator("\u0410\u043a\u0442\u0438\u0432\u043d\u044b\u0445 \u0434\u043d\u0435\u0439", str(top_target["days"])),
                ),
                counterparties=tuple(_top_counterparties(top_target["txs"], payer=True, role="\u041e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u0435\u043b\u044c", limit=6)),
                sample_transactions=tuple(_sample_transactions(top_target["txs"], limit=8)),
            )
        )

    rapid_outflow_patterns = _rapid_outflow_days(credit_txs, debit_txs)
    if rapid_outflow_patterns:
        top_rapid = rapid_outflow_patterns[0]
        warnings.append(
            ImportFraudWarning(
                code="rapid_balance_flush",
                title="\u041e\u0431\u043d\u0443\u043b\u0435\u043d\u0438\u0435 \u0431\u0430\u043b\u0430\u043d\u0441\u0430 \u0441\u0440\u0430\u0437\u0443 \u043f\u043e\u0441\u043b\u0435 \u043f\u043e\u0441\u0442\u0443\u043f\u043b\u0435\u043d\u0438\u0439",
                severity="high",
                summary=(
                    "\u0414\u0435\u043d\u044c\u0433\u0438 \u043f\u043e\u0441\u0442\u0443\u043f\u0430\u044e\u0442 \u0438 \u043f\u043e\u0447\u0442\u0438 \u0441\u0440\u0430\u0437\u0443 \u0443\u0445\u043e\u0434\u044f\u0442 \u0434\u0430\u043b\u044c\u0448\u0435. "
                    "\u0422\u0430\u043a\u043e\u0439 \u0431\u044b\u0441\u0442\u0440\u044b\u0439 \u043e\u0431\u043e\u0440\u043e\u0442 \u043f\u043e\u0445\u043e\u0436 \u043d\u0430 \u0442\u0440\u0430\u043d\u0437\u0438\u0442 \u0438\u043b\u0438 \u0441\u043a\u0440\u044b\u0442\u0438\u0435 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0430 \u043f\u0440\u043e\u0438\u0441\u0445\u043e\u0436\u0434\u0435\u043d\u0438\u044f \u0441\u0440\u0435\u0434\u0441\u0442\u0432."
                ),
                articles=("\u0441\u0442. 232-1 \u0423\u041a \u0420\u041a", "\u0441\u0442. 218 \u0423\u041a \u0420\u041a"),
                indicators=(
                    ImportFraudIndicator("\u0414\u0430\u0442\u0430", top_rapid["day"].strftime("%d.%m.%Y")),
                    ImportFraudIndicator("\u0412\u0445\u043e\u0434\u044f\u0449\u0438\u0439 \u043e\u0431\u043e\u0440\u043e\u0442", _format_money(top_rapid["credit_total"])),
                    ImportFraudIndicator("\u0418\u0441\u0445\u043e\u0434\u044f\u0449\u0438\u0439 \u043e\u0431\u043e\u0440\u043e\u0442", _format_money(top_rapid["debit_total"])),
                    ImportFraudIndicator("\u0412\u044b\u0432\u0435\u0434\u0435\u043d\u043e \u0432 \u0442\u043e\u0442 \u0436\u0435 \u0434\u0435\u043d\u044c", _format_percent(top_rapid["ratio"])),
                ),
                counterparties=tuple(
                    _top_counterparties(top_rapid["credits"], payer=True, role="\u041e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u0435\u043b\u044c", limit=3)
                    + _top_counterparties(top_rapid["debits"], payer=False, role="\u041f\u043e\u043b\u0443\u0447\u0430\u0442\u0435\u043b\u044c", limit=3)
                ),
                sample_transactions=tuple(_sample_transactions(top_rapid["credits"] + top_rapid["debits"], limit=8)),
            )
        )

    purpose_mismatch_txs = _purpose_mismatch_transactions(txs)
    if len(purpose_mismatch_txs) >= 3:
        warnings.append(
            ImportFraudWarning(
                code="purpose_mismatch",
                title="\u041d\u0430\u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u043f\u043b\u0430\u0442\u0435\u0436\u0430 \u043d\u0435 \u0441\u043e\u043e\u0442\u0432\u0435\u0442\u0441\u0442\u0432\u0443\u0435\u0442 \u043f\u043e\u0432\u0435\u0434\u0435\u043d\u0438\u044e \u043e\u043f\u0435\u0440\u0430\u0446\u0438\u0439",
                severity="medium",
                summary=(
                    "\u0412 \u043e\u043f\u0435\u0440\u0430\u0446\u0438\u044f\u0445 \u0432\u0441\u0442\u0440\u0435\u0447\u0430\u044e\u0442\u0441\u044f \u00ab\u043b\u0438\u0447\u043d\u044b\u0435 \u043f\u0435\u0440\u0435\u0432\u043e\u0434\u044b\u00bb \u0438\u043b\u0438 \u0441\u0445\u043e\u0436\u0438\u0435 \u043d\u0435\u0439\u0442\u0440\u0430\u043b\u044c\u043d\u044b\u0435 \u043e\u043f\u0438\u0441\u0430\u043d\u0438\u044f, "
                    "\u043d\u043e \u0441\u0443\u043c\u043c\u044b \u0438 \u0447\u0430\u0441\u0442\u043e\u0442\u0430 \u043f\u043e\u0445\u043e\u0436\u0438 \u043d\u0430 \u043a\u043e\u043c\u043c\u0435\u0440\u0447\u0435\u0441\u043a\u0443\u044e \u0438\u043b\u0438 \u0442\u0440\u0430\u043d\u0437\u0438\u0442\u043d\u0443\u044e \u0441\u0445\u0435\u043c\u0443."
                ),
                articles=("\u0441\u0442. 190 \u0423\u041a \u0420\u041a", "\u0441\u0442. 232-1 \u0423\u041a \u0420\u041a"),
                indicators=(
                    ImportFraudIndicator("\u041f\u043e\u0434\u043e\u0437\u0440\u0438\u0442\u0435\u043b\u044c\u043d\u044b\u0445 \u043e\u043f\u0438\u0441\u0430\u043d\u0438\u0439", str(len(purpose_mismatch_txs))),
                    ImportFraudIndicator("\u0421\u0430\u043c\u0430\u044f \u043a\u0440\u0443\u043f\u043d\u0430\u044f \u0441\u0443\u043c\u043c\u0430", _format_money(max(_amount_abs(tx) for tx in purpose_mismatch_txs))),
                    ImportFraudIndicator("\u0420\u0435\u0433\u0443\u043b\u044f\u0440\u043d\u043e\u0441\u0442\u044c \u043f\u0430\u0442\u0442\u0435\u0440\u043d\u0430", _format_percent(len(purpose_mismatch_txs) / len(txs))),
                ),
                counterparties=tuple(_top_counterparties(purpose_mismatch_txs, payer=_credit_amount(purpose_mismatch_txs[0]) > 0, role="\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442", limit=6)),
                sample_transactions=tuple(_sample_transactions(_top_by_amount(purpose_mismatch_txs, _amount_abs, 8), limit=8)),
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
            sender_name=str(tx.payer_name or "").strip(),
            sender_iin_bin=str(tx.payer_iin_bin or "").strip(),
            sender_account=str(tx.payer_account or "").strip(),
            recipient_name=str(tx.receiver_name or "").strip(),
            recipient_iin_bin=str(tx.receiver_iin_bin or "").strip(),
            recipient_account=str(tx.receiver_account or "").strip(),
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


def _repeated_amount_groups(txs: list[ImportedTransactionSample]) -> list[dict]:
    groups: dict[tuple[str, float], list[ImportedTransactionSample]] = defaultdict(list)
    for tx in txs:
        amount = round(_amount_abs(tx), 2)
        if amount <= 0:
            continue
        direction = "credit" if _credit_amount(tx) > 0 else "debit"
        groups[(direction, amount)].append(tx)

    result = []
    for (_, amount), grouped_txs in groups.items():
        if len(grouped_txs) < 4:
            continue
        result.append({
            "amount": amount,
            "count": len(grouped_txs),
            "txs": sorted(grouped_txs, key=_tx_dt_sort_key, reverse=True),
        })
    return sorted(result, key=lambda item: (item["count"], item["amount"]), reverse=True)


def _high_activity_days(txs: list[ImportedTransactionSample]) -> list[dict]:
    by_day: dict[date, list[ImportedTransactionSample]] = defaultdict(list)
    for tx in txs:
        tx_day = _tx_day(tx)
        if tx_day is not None:
            by_day[tx_day].append(tx)

    result = []
    for tx_day, day_txs in by_day.items():
        if len(day_txs) < 10:
            continue
        counterparties = {
            _counterparty_key_for_tx(tx, payer=_credit_amount(tx) > 0)
            for tx in day_txs
            if _counterparty_key_for_tx(tx, payer=_credit_amount(tx) > 0)
        }
        result.append({
            "day": tx_day,
            "count": len(day_txs),
            "turnover": sum(_amount_abs(tx) for tx in day_txs),
            "distinct_counterparties": len(counterparties),
            "txs": sorted(day_txs, key=_tx_dt_sort_key, reverse=True),
        })
    return sorted(result, key=lambda item: (item["count"], item["turnover"]), reverse=True)


def _repeated_counterparty_relationships(txs: list[ImportedTransactionSample]) -> list[dict]:
    stats: dict[str, dict] = defaultdict(lambda: {"name": "", "identifier": "", "count": 0, "turnover": 0.0, "days": set(), "txs": []})
    for tx in txs:
        is_credit = _credit_amount(tx) > 0
        key = _counterparty_key_for_tx(tx, payer=is_credit)
        if not key:
            continue
        stats[key]["name"] = stats[key]["name"] or _counterparty_label(tx, payer=is_credit)
        stats[key]["identifier"] = stats[key]["identifier"] or _counterparty_identifier(tx, payer=is_credit)
        stats[key]["count"] += 1
        stats[key]["turnover"] += _amount_abs(tx)
        tx_day = _tx_day(tx)
        if tx_day is not None:
            stats[key]["days"].add(tx_day)
        stats[key]["txs"].append(tx)

    result = []
    for item in stats.values():
        if item["count"] < 4 or item["turnover"] < 500_000:
            continue
        result.append({
            "name": item["name"] or "\u041d\u0435 \u0443\u043a\u0430\u0437\u0430\u043d",
            "identifier": item["identifier"] or "\u2014",
            "count": item["count"],
            "turnover": item["turnover"],
            "days": len(item["days"]),
            "txs": sorted(item["txs"], key=_tx_dt_sort_key, reverse=True),
        })
    return sorted(result, key=lambda item: (item["count"], item["turnover"]), reverse=True)


def _relationship_counterparties(items: list[dict]) -> list[ImportFraudCounterparty]:
    return [
        ImportFraudCounterparty(
            role="\u041a\u043e\u043d\u0442\u0440\u0430\u0433\u0435\u043d\u0442",
            name=item["name"],
            identifier=item["identifier"],
            transaction_count=int(item["count"]),
            turnover=_format_money(float(item["turnover"])),
        )
        for item in items
    ]


def _fan_in_targets(credit_txs: list[ImportedTransactionSample]) -> list[dict]:
    if not credit_txs:
        return []

    senders = {
        _counterparty_key_for_tx(tx, payer=True)
        for tx in credit_txs
        if _counterparty_key_for_tx(tx, payer=True)
    }
    if len(senders) < 5 or len(credit_txs) < 8:
        return []

    return [{
        "count": len(credit_txs),
        "distinct_senders": len(senders),
        "turnover": sum(_credit_amount(tx) for tx in credit_txs),
        "days": len({_tx_day(tx) for tx in credit_txs if _tx_day(tx) is not None}),
        "txs": sorted(credit_txs, key=_tx_dt_sort_key, reverse=True),
    }]


def _rapid_outflow_days(
    credit_txs: list[ImportedTransactionSample],
    debit_txs: list[ImportedTransactionSample],
) -> list[dict]:
    credits_by_day: dict[date, list[ImportedTransactionSample]] = defaultdict(list)
    debits_by_day: dict[date, list[ImportedTransactionSample]] = defaultdict(list)
    for tx in credit_txs:
        tx_day = _tx_day(tx)
        if tx_day is not None:
            credits_by_day[tx_day].append(tx)
    for tx in debit_txs:
        tx_day = _tx_day(tx)
        if tx_day is not None:
            debits_by_day[tx_day].append(tx)

    result = []
    for tx_day, day_credits in credits_by_day.items():
        day_debits = debits_by_day.get(tx_day, [])
        if not day_debits:
            continue
        credit_total = sum(_credit_amount(tx) for tx in day_credits)
        debit_total = sum(_debit_amount(tx) for tx in day_debits)
        if credit_total < 300_000 or debit_total < 200_000:
            continue
        ratio = debit_total / credit_total if credit_total > 0 else 0.0
        if ratio < 0.80:
            continue
        result.append({
            "day": tx_day,
            "credit_total": credit_total,
            "debit_total": debit_total,
            "ratio": ratio,
            "credits": sorted(day_credits, key=_tx_dt_sort_key, reverse=True),
            "debits": sorted(day_debits, key=_tx_dt_sort_key, reverse=True),
        })
    return sorted(result, key=lambda item: (item["ratio"], item["debit_total"]), reverse=True)


def _purpose_mismatch_transactions(txs: list[ImportedTransactionSample]) -> list[ImportedTransactionSample]:
    keywords = (
        "\u043b\u0438\u0447\u043d",
        "\u0447\u0430\u0441\u0442\u043d",
        "\u0431\u0435\u0437\u0432\u043e\u0437\u043c\u0435\u0437\u0434",
        "\u0447\u0430\u0441\u0442\u043d\u043e\u0435 \u043b\u0438\u0446\u043e",
        "personal",
        "private",
    )
    suspicious = []
    for tx in txs:
        purpose = str(tx.purpose_text or "").strip().lower()
        if not purpose:
            continue
        if not any(keyword in purpose for keyword in keywords):
            continue
        if _amount_abs(tx) < 200_000:
            continue
        suspicious.append(tx)
    return suspicious
