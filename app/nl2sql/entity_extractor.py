from __future__ import annotations

import re
from datetime import date
from typing import Optional

from .query_models import Filter, QueryEntities

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
    r"штраф|пеня|penalty|fine": "penalty",
    r"аренд|rent|лизинг|lease": "rent_lease",
    r"страхован|insurance": "insurance",
    r"дивиденд|dividend": "dividend",
    r"возврат|refund|возмещ": "refund",
    r"товар|услуг": "goods_services",
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
    "январ": 1,
    "феврал": 2,
    "март": 3,
    "апрел": 4,
    "мая": 5,
    "май": 5,
    "июн": 6,
    "июл": 7,
    "август": 8,
    "сентябр": 9,
    "октябр": 10,
    "ноябр": 11,
    "декабр": 12,
}


def _parse_amount(match: re.Match) -> Optional[float]:
    raw = match.group(1).replace(" ", "")
    try:
        value = float(raw)
    except ValueError:
        return None

    suffix = (match.group(2) or "").lower()
    if suffix in ("млн", "миллион", "m", "million"):
        value *= 1_000_000
    elif suffix in ("тыс", "тысяч", "k"):
        value *= 1_000
    return value


def _extract_year_range(question: str) -> Optional[Filter]:
    month_year = _MONTH_YEAR_RE.search(question)
    if month_year:
        import calendar

        month_prefix = month_year.group(1)[:7].lower()
        year = int(month_year.group(2))
        month = next(
            (value for key, value in _MONTH_MAP.items() if month_prefix.startswith(key)),
            None,
        )
        if month:
            last_day = calendar.monthrange(year, month)[1]
            return Filter(
                "operation_date",
                "between",
                [date(year, month, 1), date(year, month, last_day)],
            )

    year_match = _YEAR_RE.search(question)
    if year_match and re.search(r"(за|в|during|for)\s+20\d{2}", question, re.I):
        year = int(year_match.group(1))
        return Filter(
            "operation_date",
            "between",
            [date(year, 1, 1), date(year, 12, 31)],
        )
    return None


def _extract_amount_filter(question: str, existing_date_range: Optional[Filter]) -> Optional[Filter]:
    op = ">"
    if re.search(r"меньше|менее|ниже|less than|под\b", question, re.I):
        op = "<"
    elif re.search(r"равно|exactly|ровно", question, re.I):
        op = "="

    explicit_amount_context = re.search(
        r"(больше|свыше|менее|меньше|ниже|выше|от|до|на сумму|сумм[аы]|amount|kzt|usd|eur|rub|тенге|доллар|евро|руб)",
        question,
        re.I,
    )
    qualitative_large = re.search(
        r"\b(больш(ие|ой|ая)|крупн(ые|ый|ая)|large|high-value)\b",
        question,
        re.I,
    )

    amount_match = None
    for candidate in _AMOUNT_RE.finditer(question):
        raw_num = candidate.group(1).replace(" ", "")
        suffix = (candidate.group(2) or "").lower()
        if re.fullmatch(r"20\d{2}", raw_num) and not suffix:
            continue
        amount_match = candidate
        break

    if amount_match and explicit_amount_context:
        amount = _parse_amount(amount_match)
        if amount and amount >= 1_000:
            return Filter("amount_kzt", op, amount)

    if qualitative_large and existing_date_range:
        return Filter("amount_kzt", ">", 1_000_000)
    if qualitative_large:
        return Filter("amount_kzt", ">", 1_000_000)
    return None


def extract_entities(question: str) -> QueryEntities:
    normalized = question.strip()
    entities = QueryEntities()

    entities.date_range = _extract_year_range(normalized)

    if re.search(r"входящ|кредитов|credit|incoming|зачислен", normalized, re.I):
        entities.direction = Filter("direction", "=", "credit")
    elif re.search(r"исходящ|дебетов|debit|outgoing|списан", normalized, re.I):
        entities.direction = Filter("direction", "=", "debit")

    currency_match = re.search(r"\b(KZT|USD|EUR|RUB|CNY|тенге|доллар|евро|рубл)\b", normalized, re.I)
    if currency_match:
        raw_currency = currency_match.group(1).upper()
        currency_map = {
            "ТЕНГЕ": "KZT",
            "ДОЛЛАР": "USD",
            "ЕВРО": "EUR",
            "РУБЛ": "RUB",
        }
        entities.currency = Filter("currency", "=", currency_map.get(raw_currency, raw_currency))

    for pattern, canonical in _BANK_ALIASES.items():
        if re.search(pattern, normalized, re.I):
            entities.source_bank = Filter("source_bank", "=", canonical)
            break

    top_n_match = _TOP_N_RE.search(normalized)
    if top_n_match:
        entities.top_n = int(top_n_match.group(1) or top_n_match.group(2))

    for pattern, topic in _SEMANTIC_KEYWORDS.items():
        if re.search(pattern, normalized, re.I):
            entities.semantic_topic = topic
            break

    entities.amount = _extract_amount_filter(normalized, entities.date_range)
    return entities
