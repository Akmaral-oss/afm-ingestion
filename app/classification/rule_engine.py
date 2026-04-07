from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional

CAT_P2P = "P2P перевод"
CAT_STORE = "Покупка в магазине"
CAT_INTERNAL = "Внутренние операции"
CAT_CASH = "Снятие наличных"
CAT_LOAN_REP = "Погашение кредита"
CAT_GAMBLING = "Онлайн-игры / Гемблинг"
CAT_MANDATORY = "Обязательные платежи"
CAT_STATE = "Госвыплата"
CAT_SALARY = "Зарплата"
CAT_TOPUP = "Пополнение счёта"
CAT_CONTRACT = "Расчёты по договору"
CAT_INVOICE = "Оплата по счёт-фактуре"
CAT_CARD = "Платёж на карту"
CAT_FX = "Валютная операция"
CAT_LOAN_ISS = "Выдача займа"
CAT_ALIMONY = "Алименты"
CAT_SECURITIES = "Операции с ценными бумагами"
CAT_REFUND = "Возврат средств"
CAT_OTHER = "Прочее"

CATEGORY_NAMES: dict[str, str] = {
    CAT_P2P: CAT_P2P,
    CAT_STORE: CAT_STORE,
    CAT_INTERNAL: CAT_INTERNAL,
    CAT_CASH: CAT_CASH,
    CAT_LOAN_REP: CAT_LOAN_REP,
    CAT_GAMBLING: CAT_GAMBLING,
    CAT_MANDATORY: CAT_MANDATORY,
    CAT_STATE: CAT_STATE,
    CAT_SALARY: CAT_SALARY,
    CAT_TOPUP: CAT_TOPUP,
    CAT_CONTRACT: CAT_CONTRACT,
    CAT_INVOICE: CAT_INVOICE,
    CAT_CARD: CAT_CARD,
    CAT_FX: CAT_FX,
    CAT_LOAN_ISS: CAT_LOAN_ISS,
    CAT_ALIMONY: CAT_ALIMONY,
    CAT_SECURITIES: CAT_SECURITIES,
    CAT_REFUND: CAT_REFUND,
    CAT_OTHER: CAT_OTHER,
}


@dataclass(frozen=True)
class Rule:
    rule_id: str
    category: str
    priority: int
    pattern: re.Pattern
    confidence: float = 0.95


def _r(rule_id: str, category: str, priority: int, pattern: str, confidence: float = 0.95) -> Rule:
    return Rule(rule_id, category, priority, re.compile(pattern, re.IGNORECASE))


_RULES: List[Rule] = [
    _r("ALIMONY_01", CAT_ALIMONY, 200, r"алимент"),
    _r("LOAN_ISS_01", CAT_LOAN_ISS, 195, r"выдач\w*\s+(займ|кредит|заем|ссуд)"),
    _r("LOAN_REP_01", CAT_LOAN_REP, 190, r"погаш\w*\s+(кредит|займ|заем|долг|ипотек)"),
    _r("SEC_01", CAT_SECURITIES, 190, r"(ценн\w*\s+бумаг|акци|облигаци|дивиденд|брокер|\bkase\b|\bbaix\b|фондов\w+\s+рынок)"),
    _r("GAMBLING_01", CAT_GAMBLING, 188, r"\b(1xbet|betboom|olimp|pari\b|pin[\s\-]?up|melbet|fonbet|vinline|betcity|parimatch|casino|казино|букмекер|лотере|gambling|poker|покер)\b"),
    _r("INVOICE_01", CAT_INVOICE, 186, r"(сч[её]т[- ]?фактур|invoice|инвойс)"),
    _r("STATE_01", CAT_STATE, 184, r"(гос\w*\s*выплат|соц\w*\s*выплат|пенси|пособи|субсиди|енпф)"),
    _r("MAND_01", CAT_MANDATORY, 182, r"\b(кпн|ипн|иинс|осмс|соп)\b"),
    _r("MAND_02", CAT_MANDATORY, 181, r"(налог|госпошлин|штраф|пеня|обязательн\w*\s+взнос|коммунал|квартплат|жкх|электроэнерг|газоснабжен|комисси\w+\s+за)"),
    _r("FX_01", CAT_FX, 180, r"(конвертац|обмен\s+валют|forex|currency\s+exchange|валютн\w+\s+операц|покупк[аи]\s+(usd|eur|rub)|продаж[аи]\s+(usd|eur|rub))"),
    _r("CASH_01", CAT_CASH, 179, r"(снятие\s+наличн|выдача\s+наличн|получение\s+наличн|cash\s*withdrawal|cash\s*out|\batm\b|банкомат)"),
    _r("SALARY_01", CAT_SALARY, 178, r"(заработн\w*\s+плат|зарплат|аванс\s+зп|salary|payroll|жалак|выплат\w*\s+зп|\bзп\b|оклад)"),
    _r("REFUND_01", CAT_REFUND, 177, r"(возврат|refund|сторно|chargeback|чарджбек)"),
    _r("INTERNAL_01", CAT_INTERNAL, 176, r"(собственн\w+\s+средств|между\s+(своим|личн)\w*\s+счет|внутренн\w*\s+перевод|депозит|вклад|процент\w+\s+по\s+(вклад|депозит|счет))"),
    _r("CONTRACT_01", CAT_CONTRACT, 170, r"(по\s+договор|согласно\s+договор|в\s+соответствии\s+с\s+договор|оплата\s+по\s+дог|договор\s*[№#N]|оплата\s+услуг|оплата\s+работ)"),
    _r("P2P_01", CAT_P2P, 168, r"(\bp2p\b|card\s*to\s*card|перевод\s+физ\w*\s+лиц|перевод\s+между\s+карт|перевод\s+(частн\w+|физ)\s+лиц)"),
    _r("CARD_01", CAT_CARD, 166, r"(на\s+карт|платеж\s+на\s+карт|перевод\s+на\s+карт)"),
    _r("TOPUP_01", CAT_TOPUP, 164, r"(пополнен\w+\s+счет|взнос\s+наличн|cash\s*in|зачислен\w*\s+на\s+счет|приход\s+на\s+счет|пополнен)"),
    _r("STORE_01", CAT_STORE, 160, r"(оплата\s+товар|покупк\w+|qr\s*pay|\bpos\b|\bmerchant\b|торгов\w*\s+точк|через\s+терминал|оплата\s+товаров\s+и\s+услуг|магазин|супермаркет|аптек|kaspi\.kz)"),
]

_RULES.sort(key=lambda item: item.priority, reverse=True)

_NOISE = re.compile(
    r"""
      \b\d{1,2}[./\-]\d{1,2}[./\-]\d{2,4}\b
    | \bKZ[A-Z0-9]{16,}\b
    | \b\d{12}\b
    | \b[RDLN]\d{5,}\b
    | [№#]\s*\S+
    | \b\d{6,}\b
    | через\s+интернет[- ]отделение
    | через\s+мобильн\w*\s+прилож\w*
    | без\s+ндс
    | за\s+\d{1,2}/\d{1,2}/\d{4}
    | \([^)]{1,60}\)
    | \bАРН\b[:\s]*\S*
    | \bРРН\b[:\s]*\S*
    """,
    re.VERBOSE | re.IGNORECASE,
)
_SPACES = re.compile(r"\s{2,}")


def clean_purpose_text(raw: Optional[str]) -> str:
    if not raw:
        return ""
    text = _NOISE.sub(" ", raw)
    return _SPACES.sub(" ", text).strip()


@dataclass
class RuleResult:
    category_code: str
    category_name: str
    confidence: float
    rule_id: str
    source: str = "rule"


def classify_by_rules(
    purpose_text: Optional[str] = None,
    purpose_code: Optional[str] = None,
    op_type_raw: Optional[str] = None,
    direction: Optional[str] = None,
) -> RuleResult:
    cleaned = clean_purpose_text(purpose_text)
    combined = " | ".join(
        filter(
            None,
            [cleaned, (purpose_code or "").strip(), clean_purpose_text(op_type_raw), (direction or "").strip()],
        )
    )

    for rule in _RULES:
        if rule.pattern.search(combined):
            return RuleResult(
                category_code=rule.category,
                category_name=CATEGORY_NAMES[rule.category],
                confidence=rule.confidence,
                rule_id=rule.rule_id,
            )

    return RuleResult(
        category_code=CAT_OTHER,
        category_name=CATEGORY_NAMES[CAT_OTHER],
        confidence=1.0,
        rule_id="OTHER_DEFAULT",
    )
