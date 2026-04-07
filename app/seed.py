"""
Seed the database with realistic Kazakh financial transaction data.
"""
import os
import random
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Transaction, User
from .project_context import ensure_user_active_project
from .security import hash_password

COMPANIES = [
    ("РўРћРћ \"РђР»РјР° РўСЂРµР№Рґ\"", "780425301298", "KZ12345678901234"),
    ("РђРћ \"РљР°Р·Р­РЅРµСЂРіРѕ\"", "041240001234", "KZ55512340067890"),
    ("РРџ РРІР°РЅРѕРІ Рђ.РЎ.", "910312450178", "KZ98765432109876"),
    ("РўРћРћ \"РђСЃС‚Р°РЅР° РЎС‚СЂРѕР№\"", "050640009876", "KZ22233344455566"),
    ("РРџ РЎРµСЂРіРµРµРІ РљРњ.", "860715302456", "KZ11122233344455"),
    ("РђРћ \"РљР°Р·Р°С…С‚РµР»РµРєРѕРј\"", "970140005678", "KZ66677788899900"),
    ("РўРћРћ \"РќСѓСЂ Р¤Р°СЂРј\"", "120840003456", "KZ33344455566677"),
    ("РРџ РќР°Р·Р°СЂР±Р°РµРІР° Рњ.Рљ.", "880920301567", "KZ44455566677788"),
    ("РўРћРћ \"РўСЂР°РЅСЃР›РѕРіРёСЃС‚РёРє\"", "030340007890", "KZ77788899900011"),
    ("РђРћ \"РЎР°РјСЂСѓРє-Р­РЅРµСЂРіРѕ\"", "060540002345", "KZ88899900011122"),
    ("РўРћРћ \"РњРµРґСЃРµСЂРІРёСЃ РџР»СЋСЃ\"", "100240004567", "KZ99900011122233"),
    ("РРџ РљРёРј Р’.Рђ.", "830610301890", "KZ00011122233344"),
    ("РўРћРћ \"РђРіСЂРѕРџСЂРѕРј\"", "071140006789", "KZ11100022233344"),
    ("РђРћ \"Kaspi Bank\"", "980240001111", "KZ55500011122233"),
    ("РўРћРћ \"Р¦РёС„СЂРѕРІС‹Рµ Р РµС€РµРЅРёСЏ\"", "150940002222", "KZ66600011122233"),
]

PURPOSES = [
    "РћРїР»Р°С‚Р° Р·Р° РїРѕСЃС‚Р°РІРєСѓ РєР°РЅС†РµР»СЏСЂСЃРєРёС… С‚РѕРІР°СЂРѕРІ РїРѕ СЃС‡РµС‚Сѓ в„–{inv} РѕС‚ {d}",
    "Р’РѕР·РІСЂР°С‚ РїРµСЂРµРїР»Р°С‚С‹ РїРѕ РґРѕРіРѕРІРѕСЂСѓ СЌР»РµРєС‚СЂРѕСЃРЅР°Р±Р¶РµРЅРёСЏ в„–{inv}-Р­",
    "РћРїР»Р°С‚Р° РїРѕ РґРѕРіРѕРІРѕСЂСѓ Р°СЂРµРЅРґС‹ РѕС„РёСЃР° Р·Р° {month} {year} Рі.",
    "РџРµСЂРµС‡РёСЃР»РµРЅРёРµ Р·Р° С‚СЂР°РЅСЃРїРѕСЂС‚РЅС‹Рµ СѓСЃР»СѓРіРё, Р°РєС‚ в„–{inv}",
    "РћРїР»Р°С‚Р° Р·Р° РјРµРґРёС†РёРЅСЃРєРёРµ СѓСЃР»СѓРіРё, СЃС‡РµС‚ в„–{inv} РѕС‚ {d}",
    "РћРїР»Р°С‚Р° Р·Р° С‚РµР»РµРєРѕРјРјСѓРЅРёРєР°С†РёРѕРЅРЅС‹Рµ СѓСЃР»СѓРіРё Р·Р° {month} {year} Рі.",
    "Р’РѕР·РІСЂР°С‚ СЃСЂРµРґСЃС‚РІ РїРѕ РЅРµРєР°С‡РµСЃС‚РІРµРЅРЅРѕРјСѓ С‚РѕРІР°СЂСѓ в„–{inv}",
    "РћРїР»Р°С‚Р° Р·Р° СЃС‚СЂРѕРёС‚РµР»СЊРЅС‹Рµ РјР°С‚РµСЂРёР°Р»С‹, РЅР°РєР»Р°РґРЅР°СЏ в„–{inv}",
    "РџРµСЂРµС‡РёСЃР»РµРЅРёРµ РїРѕ РґРѕРіРѕРІРѕСЂСѓ РїРѕСЃС‚Р°РІРєРё в„–{inv} РѕС‚ {d}",
    "РћРїР»Р°С‚Р° Р·Р° IT-СѓСЃР»СѓРіРё (РїРѕРґРґРµСЂР¶РєР°), Р°РєС‚ в„–{inv} РѕС‚ {d}",
    "РћРїР»Р°С‚Р° Р·Р° С„Р°СЂРјР°С†РµРІС‚РёС‡РµСЃРєСѓСЋ РїСЂРѕРґСѓРєС†РёСЋ, СЃС‡РµС‚ в„–{inv}",
    "Р’РѕР·РІСЂР°С‚ Р·Р°Р»РѕРіР° РїРѕ РґРѕРіРѕРІРѕСЂСѓ Р°СЂРµРЅРґС‹ в„–{inv}",
    "РћРїР»Р°С‚Р° Р·Р° Р»РѕРіРёСЃС‚РёС‡РµСЃРєРёРµ СѓСЃР»СѓРіРё, РўРўРќ в„–{inv}",
    "РџРµСЂРµС‡РёСЃР»РµРЅРёРµ Р·Р°СЂРїР»Р°С‚С‹ СЃРѕС‚СЂСѓРґРЅРёРєР°Рј Р·Р° {month} {year} Рі.",
    "РћРїР»Р°С‚Р° Р·Р° СЃРµР»СЊСЃРєРѕС…РѕР·СЏР№СЃС‚РІРµРЅРЅСѓСЋ РїСЂРѕРґСѓРєС†РёСЋ, РґРѕРіРѕРІРѕСЂ в„–{inv}",
]

MONTHS_RU = [
    "СЏРЅРІР°СЂСЊ", "С„РµРІСЂР°Р»СЊ", "РјР°СЂС‚", "Р°РїСЂРµР»СЊ", "РјР°Р№", "РёСЋРЅСЊ",
    "РёСЋР»СЊ", "Р°РІРіСѓСЃС‚", "СЃРµРЅС‚СЏР±СЂСЊ", "РѕРєС‚СЏР±СЂСЊ", "РЅРѕСЏР±СЂСЊ", "РґРµРєР°Р±СЂСЊ",
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
    transactions = []
    start_date = datetime(2026, 1, 4)
    end_date = datetime(2026, 2, 12)
    delta = (end_date - start_date).total_seconds()

    for _ in range(count):
        dt = start_date + timedelta(seconds=random.randint(0, int(delta)))
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
        else:
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

    transactions.sort(key=lambda t: t["date"])
    return transactions


async def seed_admin_if_missing(session: AsyncSession):
    admin_email = os.getenv("ADMIN_EMAIL", "myadmin@local")
    admin_password = os.getenv("ADMIN_PASSWORD", "123123")
    res = await session.execute(select(User).where(User.email == admin_email))
    existing = res.scalar_one_or_none()
    if existing:
        # Keep the seeded admin password in sync with the current env so local
        # resets and DB restores do not silently strand the login screen.
        existing.password_hash = hash_password(admin_password)
        await ensure_user_active_project(session, existing)
        await session.commit()
        return

    user = User(
        email=admin_email,
        password_hash=hash_password(admin_password),
        role="admin",
    )
    session.add(user)
    await session.flush()
    await ensure_user_active_project(session, user)
    await session.commit()


async def seed_if_empty(session: AsyncSession, count: int = 200) -> None:
    await seed_admin_if_missing(session)

    result = await session.execute(select(func.count(Transaction.id)))
    existing = result.scalar()
    if existing and existing > 0:
        return

    admin_email = os.getenv("ADMIN_EMAIL", "myadmin@local")
    admin = (await session.execute(select(User).where(User.email == admin_email))).scalar_one()
    project = await ensure_user_active_project(session, admin)

    print(f"[seed] Database is empty — generating {count} transactions …")
    rows = _generate_transactions(count)
    for row in rows:
        row["project_id"] = project.project_id
        session.add(Transaction(**row))
    await session.commit()
    print(f"[seed] Inserted {count} transactions.")
