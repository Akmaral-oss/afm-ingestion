"""
app/classification/rule_engine.py — v4.1
Исправлены баги найденные в анализе CSV:
  BUG1: SEC_01 — паттерн акц\w+ ловил "АРН:" → заменён на точные слова
  BUG2: SALARY перехватывал "зп карта → депозит" → добавлено исключение
  BUG3: FX_02 — \bKZT\b слишком широко → убран, заменён контекстом
  BUG4: MAND_01 — "в т.ч. НДС" в сервисных платежах → уточнён паттерн
  BUG5: LOAN_ISS не ловил "перечисление суммы займа" → добавлен LOAN_ISS_04
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional

from app.utils.text_utils import repair_mojibake

# ── Category codes ────────────────────────────────────────────────────────────

CAT_P2P        = "P2P_TRANSFER"
CAT_STORE      = "STORE_PURCHASE"
CAT_INTERNAL   = "INTERNAL_OPERATION"
CAT_CASH       = "CASH_WITHDRAWAL"
CAT_LOAN_REP   = "LOAN_REPAYMENT"
CAT_GAMBLING   = "GAMBLING"
CAT_MANDATORY  = "MANDATORY_PAYMENT"
CAT_STATE      = "STATE_PAYMENT"
CAT_SALARY     = "SALARY"
CAT_TOPUP      = "ACCOUNT_TOPUP"
CAT_CASH_TOPUP = "CASH_TOPUP"
CAT_DEPOSIT    = "DEPOSIT"
CAT_CONTRACT   = "CONTRACT_SETTLEMENT"
CAT_INVOICE    = "INVOICE_PAYMENT"
CAT_CARD       = "CARD_PAYMENT"
CAT_FX         = "FX_OPERATION"
CAT_LOAN_ISS   = "LOAN_ISSUANCE"
CAT_ALIMONY    = "ALIMONY"
CAT_SECURITIES = "SECURITIES"
CAT_REFUND     = "REFUND"
CAT_OTHER      = "OTHER"

CATEGORY_NAMES: dict[str, str] = {
    CAT_P2P:        "P2P перевод",
    CAT_STORE:      "Оплата товаров и услуг",
    CAT_INTERNAL:   "Внутренние операции",
    CAT_CASH:       "Снятие наличных",
    CAT_LOAN_REP:   "Погашение кредита",
    CAT_GAMBLING:   "Онлайн-игры / Гемблинг",
    CAT_MANDATORY:  "Обязательные платежи",
    CAT_STATE:      "Госвыплата",
    CAT_SALARY:     "Зарплата",
    CAT_TOPUP:      "Пополнение счёта",
    CAT_CONTRACT:   "Расчёты по договору",
    CAT_DEPOSIT:    "Депозит",
    CAT_INVOICE:    "Оплата по счёт-фактуре",
    CAT_CARD:       "Платёж на карту",
    CAT_FX:         "Валютная операция",
    CAT_LOAN_ISS:   "Выдача займа",
    CAT_ALIMONY:    "Алименты",
    CAT_SECURITIES: "Операции с ценными бумагами",
    CAT_REFUND:     "Возврат средств",
    CAT_OTHER:      "Прочее",
}

CATEGORY_NAMES[CAT_CASH_TOPUP] = "Пополнение наличными"


@dataclass(frozen=True)
class Rule:
    rule_id:    str
    category:   str
    priority:   int
    pattern:    re.Pattern
    confidence: float = 0.95


def _r(rule_id: str, cat: str, pri: int, pat: str, conf: float = 0.95) -> Rule:
    return Rule(rule_id, cat, pri, re.compile(pat, re.IGNORECASE), conf)


_RULES: List[Rule] = [

    # Project-specific bank statement mappings to reduce "Прочее".
    _r("USR_DEP_FEE_01", CAT_DEPOSIT, 215, r"ведение\s+вкладного\s+сч[её]та|ндс\s+не\s+облагается.{0,80}вклад|согласно\s+тарифов?.{0,40}вклад"),
    _r("USR_DEP_INCOME_01", CAT_DEPOSIT, 214, r"взнос\s+во\s+вклад|плат[её]ж\s+на\s+deposit|плат[её]ж\s+о\s+deposit|досрочное\s+расторжение\s+по\s+вкладу|deposit\s+branch"),
    _r("USR_INTERNAL_01", CAT_INTERNAL, 214, r"комисси[яи]\s+за\s+перевод\s+в\s+тенге|переводы?\s+клиентом\s+денег\s+со\s+своего\s+текущего\s+сч[её]та.{0,80}на\s+свой\s+текущий\s+сч[её]т|удержание\s+суммы\s+ранее\s+выплаченного\s+вознаграждения|cardfeemonthly"),
    _r("USR_INTERNAL_02", CAT_INTERNAL, 213, r"со\s+своего\s+текущего\s+сч[её]та\s+в\s+одном\s+банке\s+на\s+свой\s+текущий\s+сч[её]т\s+в\s+другом\s+банке"),
    _r("USR_P2P_01", CAT_P2P, 214, r"перевод\s+с\s+текущего\s+сч[её]та\s+с\s+использованием\s+перевода\s+с\s+текущего\s+сч[её]та\s+на\s+чужой\s+карточный\s+сч[её]т|перевод\s+в\s+ббу"),
    _r("USR_CASH_01", CAT_CASH, 214, r"выдано\s+со\s+сч[её]та\s+клиента|снятие\s+с\s+долгосрочного\s+вклада|cash\s+dispense|cash\s+auto\s+kassa|cash\s+tcbo"),
    _r("USR_FX_01", CAT_FX, 214, r"за\s+проданные\s+по\s+курсу"),
    _r("USR_TOPUP_01", CAT_TOPUP, 214, r"soa_credit\s+popolnenie\s+kartochnogo\s+scheta\s+cherez\s+platezhnyi\s+terminal"),

    _r("HALYK_RETAIL_01", CAT_STORE, 205, r"\bretail\b"),
    _r("HALYK_CH_PAYMENT_01", CAT_CARD, 204, r"\bch\s*payment\b"),
    _r("HALYK_CH_DEBIT_01", CAT_CARD, 204, r"\bch\s*debit\b"),

    # ─── Алименты ─────────────────────────────────────────────────────────────
    _r("ALIMONY_01",    CAT_ALIMONY,    200, r"алимент"),

    # ─── Выдача займа ─────────────────────────────────────────────────────────
    _r("LOAN_ISS_01",   CAT_LOAN_ISS,   190, r"выдач\w*\s+(займ|кредит|заем|ссуд)"),
    _r("LOAN_ISS_02",   CAT_LOAN_ISS,   190, r"предоставлен\w*\s+(займ|кредит|ссуд)"),
    _r("LOAN_ISS_03",   CAT_LOAN_ISS,   185, r"отнесено\s+в\s+займ"),
    # FIX BUG5: "перечисление суммы займа на счет торговой организации" (Kaspi рассрочка)
    _r("LOAN_ISS_04",   CAT_LOAN_ISS,   188, r"перечислен\w+\s+сумм\w+\s+займ"),
    _r("LOAN_ISS_05",   CAT_LOAN_ISS,   185, r"микрокредит\w*\s+физическ"),

    # ─── Погашение кредита ────────────────────────────────────────────────────
    _r("LOAN_REP_01",   CAT_LOAN_REP,   190, r"погашен\w*\s+(кредит|займ|заем|долг|задолженност|ипотек)"),
    _r("LOAN_REP_02",   CAT_LOAN_REP,   185, r"(возврат|выплат)\w*\s+(кредит|займ|долг)"),
    _r("LOAN_REP_03",   CAT_LOAN_REP,   180, r"ипотек"),
    _r("LOAN_REP_04",   CAT_LOAN_REP,   182, r"резервирован\w+\s+средств.{0,30}погашен"),

    # ─── Ценные бумаги ────────────────────────────────────────────────────────
    # FIX BUG1: убраны "акц\w+" и "облигац" как одиночные — слишком широко.
    # Теперь только точные устойчивые словосочетания.
    _r("SEC_01",        CAT_SECURITIES, 190,
       r"(ценн\w*\s+бумаг|акции\s+(купл|продаж|торг)|облигаци\w+\s+(купл|погаш)|"
       r"дивиденд|брокер|\bkase\b|\baix\b|фондов\w+\s+(рынок|биржа))"),

    # ─── Гемблинг ─────────────────────────────────────────────────────────────
    _r("GAMB_01",       CAT_GAMBLING,   190,
       r"\b(1xbet|betboom|olimp|pari\b|pin[\s\-]?up|melbet|fonbet|"
       r"vinline|betcity|parimatch|casino|казино|букмекер|лотере|gambling|poker|покер)\b"),

    # ─── Оплата по счёт-фактуре ───────────────────────────────────────────────
    _r("INV_01",        CAT_INVOICE,    190, r"(счет[- ]?фактур|счёт[- ]?фактур|invoice|инвойс)"),

    # ─── Госвыплата ───────────────────────────────────────────────────────────
    _r("STATE_01",      CAT_STATE,      190,
       r"(гос\w*\s*выплат|соц\w*\s*выплат|единые?\s+пенсионные?\s+выплат|"
       r"пенсионные?\s+(выплат|начислен)|пособи\w+\s*(по\s+)?безработиц)"),
    _r("STATE_02",      CAT_STATE,      185, r"(пенсия|пенсионн\w+|пособие|субсиди|соцвыплат|\bенпф\b)"),

    # ─── Обязательные платежи ─────────────────────────────────────────────────
    _r("MAND_01",       CAT_MANDATORY,  190, r"\b(кпн|ипн|иис|осмс|соп)\b"),
    # FIX BUG4: убран "ндс" как standalone — слишком широко.
    # Теперь ловим только явные налоговые платежи, не "В Т.Ч. НДС" в сервисных платёжках.
    _r("MAND_02",       CAT_MANDATORY,  188,
       r"(уплата\s+ндс|оплата\s+ндс|перечислен\w+\s+ндс|ндс\s+за\s|\bндс\s+от\s)"),
    _r("MAND_03",       CAT_MANDATORY,  185,
       r"(налог|госпошлин|штраф|пеня|обязательн\w*\s+взнос|"
       r"коммунал|квартплат|жкх|водоснабжен|теплоснабжен|электроэнерг|газоснабжен)"),
    _r("MAND_04",       CAT_MANDATORY,  183, r"комисси\w+\s+(за\s+)?операц"),

    # ─── Валютная операция ────────────────────────────────────────────────────
    # FIX BUG3: убран FX_02 (\bKZT\b) — слишком широко, срабатывал на выплаты по вкладу
    _r("FX_01",         CAT_FX,         185, r"(конвертац|обмен\s+валют|forex|currency\s+exchange)"),
    _r("FX_03",         CAT_FX,         175,
       r"(покупк[аи]\s+(usd|eur|rub)|продаж[аи]\s+(usd|eur|rub)|"
       r"конверти\w+|валютн\w+\s+(операц|перевод))"),

    # ─── Снятие наличных ──────────────────────────────────────────────────────
    _r("CASH_01",       CAT_CASH,       185,
       r"(снятие\s+наличн|выдача\s+наличн|получение\s+наличн|cash\s*withdrawal|cash\s*out|\batm\b|банкомат)"),
    _r("CASH_02",       CAT_CASH,       170, r"наличн"),

    # ─── Зарплата ─────────────────────────────────────────────────────────────
    # FIX BUG2: если "зп карта" И "депозит" → INTERNAL, не SALARY
    # Решение: INTERNAL_OPERATION с "зп карт" + "депозит" имеет приоритет 188 — выше SAL_01(185)
    _r("INT_ZP_DEP",    CAT_INTERNAL,   188,
       r"(зп\s+карт|зарплатн\w+\s+карт).{0,40}(депозит|вклад)|"
       r"(депозит|вклад).{0,40}(зп\s+карт|зарплатн\w+\s+карт)"),
    _r("SAL_01",        CAT_SALARY,     185,
       r"(заработн\w*\s+плат|зарплат|аванс\s+зп|зп\s+карт|\bsalary\b|payroll|жалақы|выплат\w*\s+зп)"),
    _r("SAL_02",        CAT_SALARY,     178, r"(\bзп\b|оклад)"),

    # ─── Возврат средств ──────────────────────────────────────────────────────
    _r("REF_01",        CAT_REFUND,     185, r"(возврат|refund|сторно|chargeback|чарджбек)"),

    # ─── Причисление процентов по вкладу → INTERNAL (не FX, не INVOICE) ──────
    # FIX BUG3+BUG4: "причисление процентов / выплата вознаграждения по вкладу"
    _r("INT_DEPOSIT",   CAT_INTERNAL,   192,
       r"(причислени\w+\s+процент|выплат\w+\s+вознагражден\w+\s+по\s+вклад|"
       r"процент\w+\s+по\s+(вклад|депозит|счет))"),

    # ─── Внутренние операции ──────────────────────────────────────────────────
    _r("INT_01",        CAT_INTERNAL,   182,
       r"(собственн\w+\s+средств|между\s+(своим|личн)\w*\s+счет|перенос\s+сумм|внутренн\w*\s+перевод)"),
    _r("INT_02",        CAT_INTERNAL,   178,
       r"(со\s+счета\s+на\s+счет|с\s+депозит\w*\s+на|на\s+депозит|"
       r"с\s+зп\s+карт\w*\s+на\s+депозит|с\s+карт\w*\s+на\s+депозит)"),
    _r("INT_03",        CAT_INTERNAL,   165, r"депозит"),

    # ─── Расчёты по договору ──────────────────────────────────────────────────
    _r("CONT_01",       CAT_CONTRACT,   175,
       r"(по\s+договор|согласно\s+договор|в\s+соответствии\s+с\s+договор|оплата\s+по\s+дог)"),
    _r("CONT_02",       CAT_CONTRACT,   165, r"договор\s*[№#N]"),

    # ─── P2P перевод ──────────────────────────────────────────────────────────
    _r("P2P_01",        CAT_P2P,        175, r"\bp2p\b"),
    _r("P2P_02",        CAT_P2P,        172, r"card\s*to\s*card|cald\s*to\s*cald"),
    _r("P2P_03",        CAT_P2P,        168,
       r"(перевод\s+физ\w*\s+лиц|перевод\s+между\s+карт|перевод\s+(частн\w+|физ)\s+лиц)"),

    # ─── Платёж на карту ──────────────────────────────────────────────────────
    _r("CARD_01",       CAT_CARD,       162,
       r"(на\s+карту|на\s+карт\w+|платеж\s+на\s+карт|перевод\s+на\s+карт)"),

    # ─── Пополнение счёта ─────────────────────────────────────────────────────
    _r("TOPUP_01",      CAT_TOPUP,      165,
       r"(пополнен\w+\s+счет|взнос\s+наличн|cash\s*in|зачислен\w*\s+на\s+счет|приход\s+на\s+счет)"),
    _r("TOPUP_02",      CAT_TOPUP,      155, r"пополнен"),

    # ─── Покупка в магазине ───────────────────────────────────────────────────
    _r("CASH_TOPUP_01", CAT_CASH_TOPUP, 176,
       "(\\u0440\\u0435\\u0441\\u0430\\u0439\\u043a\\u043b\\u0435\\u0440|recycler|\\u043f\\u043e\\u0441\\u0442\\u0443\\u043f\\u043b\\u0435\\u043d\\u0438\\u0435\\s+\\u0434\\u0435\\u043d\\u0435\\u0436\\u043d\\u044b\\u0445\\s+\\u0441\\u0440\\u0435\\u0434\\u0441\\u0442\\u0432\\s+\\u0447\\u0435\\u0440\\u0435\\u0437\\s+\\u0440\\u0435\\u0441\\u0430\\u0439\\u043a\\u043b\\u0435\\u0440)"),
    _r("STORE_01",      CAT_STORE,      160,
       r"(оплата\s+товар|покупк\w+|qr\s*pay|\bpos\b|\bmerchant\b|торгов\w*\s+точк|через\s+терминал)"),
    # "оплата товаров и услуг" = явная покупка
    _r("STORE_03",      CAT_STORE,      158, r"оплата\s+товаров\s+и\s+услуг"),
    _r("STORE_02",      CAT_STORE,      150,
       r"(продажи\s+с\s|kaspi\.kz|kz\s+продаж|магазин|супермаркет|аптек)"),

    # ─── Общая "оплата за…" → CONTRACT ────────────────────────────────────────
    _r("GEN_OPL",       CAT_CONTRACT,   100, r"(оплата\s+за\s+|оплата\s+услуг|оплата\s+работ)"),
]

_RULES.sort(key=lambda r: r.priority, reverse=True)


# ── Purpose-text cleaner ──────────────────────────────────────────────────────

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
    t = _NOISE.sub(" ", repair_mojibake(raw))
    return _SPACES.sub(" ", t).strip()


# ── Result + classifier ───────────────────────────────────────────────────────

@dataclass
class RuleResult:
    category_code: str
    category_name: str
    confidence:    float
    rule_id:       str
    source:        str = "rule"


def classify_by_rules(
    purpose_text:    Optional[str] = None,
    purpose_code:    Optional[str] = None,
    op_type_raw:     Optional[str] = None,
    direction:       Optional[str] = None,
) -> RuleResult:
    cleaned  = clean_purpose_text(purpose_text)
    combined = " | ".join(filter(None, [
        cleaned,
        (purpose_code or "").strip(),
        clean_purpose_text(op_type_raw),
    ]))

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
        rule_id="DEFAULT_OTHER",
    )
