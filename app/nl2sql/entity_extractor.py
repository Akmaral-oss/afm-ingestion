from __future__ import annotations

"""
Rule-based entity extractor.

Deterministically extracts structured entities (amounts, dates, direction,
currency, bank names, top-N) from the user question. These are injected as
hints into the LLM prompt — the LLM still writes the final SQL, but with
much higher accuracy because obvious constraints are already resolved.

This module extracts hints only. It does not generate SQL and does not apply
business logic beyond lightweight parsing heuristics.
"""

import re
from datetime import date
from typing import Optional

from .query_models import Filter, QueryEntities


# ── regex patterns ────────────────────────────────────────────────────────────

_AMOUNT_RE = re.compile(
    r"(\d[\d\s]*)\s*(млн|миллион|тыс|тысяч|k|m|million)?",
    re.IGNORECASE,
)

_YEAR_RE = re.compile(r"\b(20\d{2})\b")

_MONTH_YEAR_RE = re.compile(
    r"\b(январ|феврал|март|апрел|май|мая|июн|июл|август|сентябр|октябр|ноябр|декабр)\w*"
    r"\s+(\d{4})\b",
    re.IGNORECASE,
)

_TOP_N_RE = re.compile(
    r"\bтоп[\s-]?(\d+)\b|\b(\d+)\s+(?:топ|лучш|крупнейш)",
    re.IGNORECASE,
)

_IIN_BIN_RE = re.compile(r"\b(\d{12})\b")
_IBAN_RE = re.compile(r"\bKZ[A-Z0-9]{18,}\b", re.IGNORECASE)


# Canonical semantic topic labels.
_SEMANTIC_KEYWORDS: dict[str, str] = {
    r"долг|займ|заем|кредит|погашен|loan|repayment": "loan",
    r"депозит|вклад|процент|вознагражден": "deposit",
    r"налог|ндс|кпн|tax": "tax",
    r"зарплат|оклад|salary|payroll": "salary",
    r"комисси|fee|сбор": "fee",
    r"коммун": "utilities",
    r"перевод|transfer": "transfer",
    r"банкомат|atm|наличн|cash": "atm_cash",
    r"покупк|purchase|магазин|shop": "purchase",
    r"штраф|штрафн|пеня|penalty|fine": "penalty",
    r"аренд|rent|лизинг|lease": "rent_lease",
    r"страхован|insurance": "insurance",
    r"дивиденд|dividend": "dividend",
    r"возврат|refund|возмещ": "refund",
    r"товар|услуг": "goods_services",
    # ── AML / fraud detection patterns ───────────────────────────────────────
    r"недвижим|real.?estate|квартир|участок|property": "real_estate",
    r"схем.{0,5}ип|ип.{0,10}схем|подозрительн.{0,15}ип|индивидуальный предприниматель": "ip_entrepreneur",
    r"обнал[а-я]{0,3}|обналич|cash.?out|вывод наличн|снятие наличн": "cash_out",  # FIX: genitive «обнала»
    r"круговой|круговые|кольцев|circular|самоперевод|ring.?transact": "circular",
    r"транзитн|транзит.{0,6}счет|transit.?account|промежуточн.{0,10}счет": "transit",
    r"фиксирован|постоянн|регулярн|recurring": "recurring",
    r"подозрительн|suspicious|anomal|fraud": "suspicious",
    # ── Analytics patterns ────────────────────────────────────────────────────
    r"топ.{0,5}банк|банки.{0,10}снятию|банк.{0,10}объем|с какого банк|какой банк.{0,15}снят|больше.{0,10}снят.{0,10}банк": "top_banks",  # FIX: разговорная форма
    r"поступлени.{0,10}клиент|входящи.{0,10}клиент": "client_incoming",
    r"сумма.{0,10}за период|итого.{0,10}за период|оборот.{0,10}период": "period_summary",
    r"обнал.{0,10}тип|тип.{0,10}обнал|обнал.{0,5}по": "cash_out_by_type",
}

_BANK_ALIASES: dict[str, str] = {
    r"kaspi|каспи": "kaspi",
    r"halyk|халык|народн": "halyk",
    r"forte|форте": "forte",
    r"sber|сбер": "sber",
    r"jusan|джусан": "jusan",
    r"bereke|береке": "bereke",
    r"centercredit|центркредит": "centercredit",
    r"bcc|бцк": "bcc",
}

_MONTH_MAP: dict[str, int] = {
    "январ": 1, "феврал": 2, "март": 3, "апрел": 4,
    "мая": 5,   "май": 5,    "июн": 6,  "июл": 7,
    "август": 8, "сентябр": 9, "октябр": 10, "ноябр": 11, "декабр": 12,
}


# ── helpers ───────────────────────────────────────────────────────────────────

def _parse_amount(m: re.Match) -> Optional[float]:
    raw = m.group(1).replace(" ", "")
    try:
        value = float(raw)
    except ValueError:
        return None
    suffix = (m.group(2) or "").lower()
    if suffix in ("млн", "миллион", "m", "million"):
        value *= 1_000_000
    elif suffix in ("тыс", "тысяч", "k"):
        value *= 1_000
    return value


def _extract_year_range(q: str) -> Optional[Filter]:
    ym = _MONTH_YEAR_RE.search(q)
    if ym:
        mo_str = ym.group(1)[:7].lower()
        year = int(ym.group(2))
        month = next((v for k, v in _MONTH_MAP.items() if mo_str.startswith(k)), None)
        if month:
            import calendar
            last_day = calendar.monthrange(year, month)[1]
            return Filter("operation_date", "between",
                          [date(year, month, 1), date(year, month, last_day)])
    y = _YEAR_RE.search(q)
    if y and re.search(r"(за|в|during|for)\s+20\d{2}", q, re.I):
        year = int(y.group(1))
        return Filter("operation_date", "between",
                      [date(year, 1, 1), date(year, 12, 31)])
    return None


def _extract_amount_filter(q: str, existing_date_range: Optional[Filter]) -> Optional[Filter]:
    op = ">"
    if re.search(r"меньше|менее|ниже|less than|под\b", q, re.I):
        op = "<"
    elif re.search(r"равно|exactly|ровно", q, re.I):
        op = "="

    explicit_amount_ctx = re.search(
        r"(больше|свыше|менее|меньше|ниже|выше|от|до|на сумму|сумм[аы]|amount|kzt|usd|eur|rub|тенге|доллар|евро|руб)",
        q, re.I,
    )
    qualitative_large = re.search(
        r"\b(больш(ие|ой|ая)|крупн(ые|ый|ая)|large|high-value)\b", q, re.I,
    )

    amount_match = None
    for cand in _AMOUNT_RE.finditer(q):
        raw_num = cand.group(1).replace(" ", "")
        suffix = (cand.group(2) or "").lower()
        if re.fullmatch(r"20\d{2}", raw_num) and not suffix:
            continue
        amount_match = cand
        break

    if amount_match and explicit_amount_ctx:
        amt = _parse_amount(amount_match)
        if amt and amt >= 1_000:
            return Filter("amount_kzt", op, amt)

    if qualitative_large:
        return Filter("amount_kzt", ">", 1_000_000)

    return None


# ── public extractor ──────────────────────────────────────────────────────────

def extract_entities(question: str) -> QueryEntities:
    q = question.strip()
    entities = QueryEntities()

    entities.date_range = _extract_year_range(q)

    if re.search(r"входящ|кредитов|credit|incoming|зачислен|поступлени|поступления|приход|пополнени", q, re.I):
        entities.direction = Filter("direction", "=", "credit")
    elif re.search(r"исходящ|дебетов|debit|outgoing|списан|снятие|снятия|снял|снимали|расход", q, re.I):
        entities.direction = Filter("direction", "=", "debit")

    cur_m = re.search(r"\b(KZT|USD|EUR|RUB|CNY|тенге|доллар|евро|рубл)\b", q, re.I)
    if cur_m:
        raw_cur = cur_m.group(1).upper()
        cur_map = {"ТЕНГЕ": "KZT", "ДОЛЛАР": "USD", "ЕВРО": "EUR", "РУБЛ": "RUB"}
        entities.currency = Filter("currency", "=", cur_map.get(raw_cur, raw_cur))

    for pattern, canonical in _BANK_ALIASES.items():
        if re.search(pattern, q, re.I):
            entities.source_bank = Filter("source_bank", "=", canonical)
            break

    tn = _TOP_N_RE.search(q)
    if tn:
        entities.top_n = int(tn.group(1) or tn.group(2))

    for pattern, topic in _SEMANTIC_KEYWORDS.items():
        if re.search(pattern, q, re.I):
            entities.semantic_topic = topic
            break

    entities.amount = _extract_amount_filter(q, entities.date_range)

    return entities