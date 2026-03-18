"""
Seed the database with realistic Kazakh financial transaction data.
"""
import os
import random
from datetime import datetime, timedelta

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Transaction, User
from .security import hash_password

# ---------------------------------------------------------------------------
# Realistic Kazakh company / individual data
# ---------------------------------------------------------------------------

COMPANIES = [
    ("ТОО \"Алма Трейд\"", "780425301298", "KZ12345678901234"),
    ("АО \"КазЭнерго\"", "041240001234", "KZ55512340067890"),
    ("ИП Иванов А.С.", "910312450178", "KZ98765432109876"),
    ("ТОО \"Астана Строй\"", "050640009876", "KZ22233344455566"),
    ("ИП Сергеев К.М.", "860715302456", "KZ11122233344455"),
    ("АО \"Казахтелеком\"", "970140005678", "KZ66677788899900"),
    ("ТОО \"Нур Фарм\"", "120840003456", "KZ33344455566677"),
    ("ИП Назарбаева М.К.", "880920301567", "KZ44455566677788"),
    ("ТОО \"ТрансЛогистик\"", "030340007890", "KZ77788899900011"),
    ("АО \"Самрук-Энерго\"", "060540002345", "KZ88899900011122"),
    ("ТОО \"Медсервис Плюс\"", "100240004567", "KZ99900011122233"),
    ("ИП Ким В.А.", "830610301890", "KZ00011122233344"),
    ("ТОО \"АгроПром\"", "071140006789", "KZ11100022233344"),
    ("АО \"Kaspi Bank\"", "980240001111", "KZ55500011122233"),
    ("ТОО \"Цифровые Решения\"", "150940002222", "KZ66600011122233"),
]

PURPOSES = [
    "Оплата за поставку канцелярских товаров по счету №{inv} от {d}",
    "Возврат переплаты по договору электроснабжения №{inv}-Э",
    "Оплата по договору аренды офиса за {month} {year} г.",
    "Перечисление за транспортные услуги, акт №{inv}",
    "Оплата за медицинские услуги, счет №{inv} от {d}",
    "Оплата за телекоммуникационные услуги за {month} {year} г.",
    "Возврат средств по рекламации №{inv}",
    "Оплата за строительные материалы, накладная №{inv}",
    "Перечисление по договору поставки №{inv} от {d}",
    "Оплата за IT-услуги (поддержка), акт №{inv} от {d}",
    "Оплата за фармацевтическую продукцию, счет №{inv}",
    "Возврат залога по договору аренды №{inv}",
    "Оплата за логистические услуги, ТТН №{inv}",
    "Перечисление зарплаты сотрудникам за {month} {year} г.",
    "Оплата за сельскохозяйственную продукцию, договор №{inv}",
]

MONTHS_RU = [
    "январь", "февраль", "март", "апрель", "май", "июнь",
    "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь",
]

CURRENCIES = ["KZT", "KZT", "KZT", "KZT", "KZT", "USD", "EUR", "RUB"]


def _random_purpose(dt: datetime) -> str:
    template = random.choice(PURPOSES)
    return template.format(
        inv=random.randint(100, 9999),
        d=dt.strftime("%d.%m.%Y"),
        month=MONTHS_RU[dt.month - 1],
        year=dt.year,
    )


def _generate_transactions(count: int) -> list[dict]:
    """Generate `count` random transaction dicts."""
    transactions = []
    start_date = datetime(2026, 1, 4)
    end_date = datetime(2026, 2, 12)
    delta = (end_date - start_date).total_seconds()

    for _ in range(count):
        dt = start_date + timedelta(seconds=random.randint(0, int(delta)))
        # Ensure business hours (8:00 - 18:00)
        dt = dt.replace(hour=random.randint(8, 17), minute=random.randint(0, 59))

        sender = random.choice(COMPANIES)
        recipient = random.choice([c for c in COMPANIES if c != sender])

        is_debit = random.random() < 0.55
        currency = random.choice(CURRENCIES)

        if currency == "KZT":
            amount = round(random.uniform(50_000, 5_000_000), 2)
            amount_tenge = amount
        elif currency == "USD":
            amount = round(random.uniform(100, 10_000), 2)
            amount_tenge = round(amount * 475.50, 2)
        elif currency == "EUR":
            amount = round(random.uniform(100, 10_000), 2)
            amount_tenge = round(amount * 520.30, 2)
        else:  # RUB
            amount = round(random.uniform(10_000, 500_000), 2)
            amount_tenge = round(amount * 5.20, 2)

        transactions.append(
            {
                "date": dt,
                "sender_name": sender[0],
                "sender_iin_bin": sender[1],
                "sender_account": sender[2],
                "recipient_name": recipient[0],
                "recipient_iin_bin": recipient[1],
                "recipient_account": recipient[2],
                "purpose": _random_purpose(dt),
                "currency": currency,
                "debit": round(amount, 2) if is_debit else 0,
                "credit": 0 if is_debit else round(amount, 2),
                "amount_tenge": round(amount_tenge, 2),
            }
        )

    # Sort by date
    transactions.sort(key=lambda t: t["date"])
    return transactions

async def seed_admin_if_missing(session):
    admin_email = os.getenv("ADMIN_EMAIL", "myadmin@local")
    admin_password = os.getenv("ADMIN_PASSWORD", "123123")
    res = await session.execute(select(User).where(User.email == admin_email))
    if res.scalar_one_or_none():
        return

    session.add(User(
        email=admin_email,
        password_hash=hash_password(admin_password),
        role="admin"
    ))
    await session.commit()

async def seed_if_empty(session: AsyncSession, count: int = 200) -> None:
    """Check if the transactions table is empty; if so, populate it."""
    await seed_admin_if_missing(session)

    result = await session.execute(select(func.count(Transaction.id)))
    existing = result.scalar()
    if existing and existing > 0:
        return

    print(f"[seed] Database is empty — generating {count} transactions …")
    rows = _generate_transactions(count)
    for row in rows:
        session.add(Transaction(**row))
    await session.commit()
    print(f"[seed] Inserted {count} transactions.")
