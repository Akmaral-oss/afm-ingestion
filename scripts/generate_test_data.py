"""
Test Data Generator for Financial Crime Detection

Generates realistic transaction data for:
- Circular transactions (self-transfers)
- Real estate transactions
- IP entrepreneur patterns
- Cash out / obnal schemes
- Suspicious round amounts
- Transit accounts
- Rapid-fire patterns
"""

import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
import uuid


class TransactionGenerator:
    """Generate realistic financial transactions for testing."""

    FAKE_NAMES = {
        "payers": [
            "ТОО СМАЙЛФЕЙС", "ТОО RED BANK", "АО ЕВРАЗИЯИНВЕСТ",
            "ТОО ГАРАНТКАПИТАЛ", "АО ТРАСТБАНК", "ТОО ФИНАНС ГРУПП",
            "АО ГЛОБАЛ СЕРВИС", "ТОО КАЗАХСТАН ТИ", "АО АГРОКОМПАНИЯ",
            "ТОО ТРАНСПОРТ ЛОГИСТИКА"
        ],
        "receivers": [
            "ИП НУРЛАНОВ РУСТЕМ", "ИП СЕЙТКУЛОВА АЙНУР", "ИП МОЛДОШЕВ АДИЛЬБЕК",
            "ООО РЕХАБ ЦЕНТР", "АО НЕДВИЖИМОСТЬ ГРУПП", "ИП ТЕМИРОВА ГУЛЗИРА",
            "ТОО КОНСАЛТ СЕРВИС", "ИП САРСЕНБАЕВ МАРАТ", "АО ТРАНЗИТ ЛОГИСТИКА",
            "ТОО ИНВЕСТ ПАРТНЕРЫ"
        ],
        "real_estate": [
            "АО НЕДВИЖИМОСТЬ ПРО", "ТОО КВАРТИР МАРКЕТ", "АО ДОМОСТРОЕНИЕ",
            "ТОО УЧАСТОК АГЕНТСТВО", "АО КОММЕРЧЕСКАЯ НЕДВИЖИМОСТЬ",
            "ИП НЕДВИЖИМЫЕ АКТИВЫ"
        ]
    }

    PURPOSES = {
        "normal": [
            "Оплата счета №12345", "Поставка товаров", "Оказание услуг",
            "Арендная плата", "Коммунальные услуги", "Налоговый платеж",
            "Зарплата", "Дивиденд", "Возврат средств"
        ],
        "real_estate": [
            "Покупка квартиры", "Оплата недвижимости", "Участок земли",
            "Ремонт дома", "Строительство здания", "Дом в коттедже",
            "Квартира в ЖК", "Жилой дом", "Недвижимое имущество"
        ],
        "suspicious": [
            "Нераспределенные средства", "Без назначения", "Прочие платежи",
            "Перевод", "Между счетами", "Технический платеж", ""
        ]
    }

    ROUND_AMOUNTS = [1000000, 500000, 250000, 100000, 50000, 25000, 10000]

    @staticmethod
    def generate_circular_transactions(count: int = 20) -> List[Dict[str, Any]]:
        """Generate self-transfer transactions."""
        transactions = []
        names = TransactionGenerator.FAKE_NAMES["payers"]

        for i in range(count):
            name = random.choice(names)
            base_date = datetime.now() - timedelta(days=random.randint(1, 180))
            
            transactions.append({
                "tx_id": str(uuid.uuid4()),
                "operation_date": base_date.date(),
                "operation_ts": base_date,
                "amount_kzt": random.choice([50000, 100000, 200000, 500000]),
                "direction": "credit" if random.random() > 0.5 else "debit",
                "payer_name": name,
                "receiver_name": name,  # Self-transfer
                "purpose_text": random.choice(TransactionGenerator.PURPOSES["suspicious"]),
                "operation_type_raw": "Circular Transfer",
                "source_bank": random.choice(["HALYK", "KASPI", "EURASYA"]),
                "row_hash": str(uuid.uuid4()),
                "semantic_text": f"self-transfer {name} circular"
            })
        
        return transactions

    @staticmethod
    def generate_real_estate_transactions(count: int = 50) -> List[Dict[str, Any]]:
        """Generate real estate related transactions."""
        transactions = []
        re_receivers = TransactionGenerator.FAKE_NAMES["real_estate"]
        payers = TransactionGenerator.FAKE_NAMES["payers"]

        for i in range(count):
            base_date = datetime.now() - timedelta(days=random.randint(1, 180))
            
            transactions.append({
                "tx_id": str(uuid.uuid4()),
                "operation_date": base_date.date(),
                "operation_ts": base_date,
                "amount_kzt": random.choice([1000000, 2000000, 5000000, 10000000]),
                "direction": "debit",
                "payer_name": random.choice(payers),
                "receiver_name": random.choice(re_receivers),
                "purpose_text": random.choice(TransactionGenerator.PURPOSES["real_estate"]),
                "operation_type_raw": "Real Estate Payment",
                "source_bank": random.choice(["HALYK", "KASPI"]),
                "row_hash": str(uuid.uuid4()),
                "semantic_text": "real estate недвижимость квартира дом участок"
            })
        
        return transactions

    @staticmethod
    def generate_ip_entrepreneur_transactions(count: int = 30) -> List[Dict[str, Any]]:
        """Generate transactions with IP (individual entrepreneurs)."""
        transactions = []
        ip_receivers = [name for name in TransactionGenerator.FAKE_NAMES["receivers"] if "ИП" in name]
        payers = TransactionGenerator.FAKE_NAMES["payers"]

        for i in range(count):
            base_date = datetime.now() - timedelta(days=random.randint(1, 180))
            
            transactions.append({
                "tx_id": str(uuid.uuid4()),
                "operation_date": base_date.date(),
                "operation_ts": base_date,
                "amount_kzt": random.choice([100000, 250000, 500000, 1000000, 2000000]),
                "direction": "debit",
                "payer_name": random.choice(payers),
                "receiver_name": random.choice(ip_receivers),
                "purpose_text": random.choice(TransactionGenerator.PURPOSES["normal"]),
                "operation_type_raw": "IP Payment",
                "source_bank": random.choice(["HALYK", "KASPI"]),
                "row_hash": str(uuid.uuid4()),
                "semantic_text": "ип индивидуальный предприниматель entrepreneur"
            })
        
        return transactions

    @staticmethod
    def generate_cash_out_transactions(count: int = 100) -> List[Dict[str, Any]]:
        """Generate cash out / obnal transactions."""
        transactions = []
        receivers = TransactionGenerator.FAKE_NAMES["receivers"]
        payers = TransactionGenerator.FAKE_NAMES["payers"]

        for i in range(count):
            base_date = datetime.now() - timedelta(days=random.randint(1, 180))
            
            transactions.append({
                "tx_id": str(uuid.uuid4()),
                "operation_date": base_date.date(),
                "operation_ts": base_date,
                "amount_kzt": random.choice(TransactionGenerator.ROUND_AMOUNTS + [75000, 150000, 350000]),
                "direction": "debit",
                "payer_name": random.choice(payers),
                "receiver_name": random.choice(receivers),
                "purpose_text": random.choice(TransactionGenerator.PURPOSES["suspicious"]),
                "operation_type_raw": "Cash Out / Obnal",
                "source_bank": random.choice(["HALYK", "KASPI"]),
                "row_hash": str(uuid.uuid4()),
                "semantic_text": "обнал обналичивание cash out debit"
            })
        
        return transactions

    @staticmethod
    def generate_rapid_fire_transactions(count: int = 50) -> List[Dict[str, Any]]:
        """Generate rapid-fire transaction sequences."""
        transactions = []
        payer = random.choice(TransactionGenerator.FAKE_NAMES["payers"])
        receivers = TransactionGenerator.FAKE_NAMES["receivers"]
        
        base_time = datetime.now() - timedelta(days=random.randint(1, 30))

        for i in range(count):
            time_offset = timedelta(minutes=random.randint(0, 59))
            
            transactions.append({
                "tx_id": str(uuid.uuid4()),
                "operation_date": (base_time + time_offset).date(),
                "operation_ts": base_time + time_offset,
                "amount_kzt": random.choice([100000, 250000, 500000]),
                "direction": "debit",
                "payer_name": payer,
                "receiver_name": random.choice(receivers),
                "purpose_text": "Перевод средств",
                "operation_type_raw": "Rapid Fire",
                "source_bank": random.choice(["HALYK", "KASPI"]),
                "row_hash": str(uuid.uuid4()),
                "semantic_text": "rapid fire быстрая очередь последовательность"
            })
        
        return transactions

    @staticmethod
    def generate_transit_accounts(count: int = 40) -> List[Dict[str, Any]]:
        """Generate transactions that use intermediate accounts."""
        transactions = []
        transit_accounts = [
            "ТОО ФИНТЕХ МОСТ", "АО ПЛАТЕЖ ЦЕНТР", "ТОО ВАЛЮТА ОБМЕН",
            "АО ПРОМЕЖУТОЧНЫЙ СЕРВИС", "ТОО ЭКСПРЕСС РАСЧЕТЫ"
        ]
        payers = TransactionGenerator.FAKE_NAMES["payers"]
        receivers = TransactionGenerator.FAKE_NAMES["receivers"]

        for i in range(count // 2):
            base_date = datetime.now() - timedelta(days=random.randint(1, 180))
            transit_account = random.choice(transit_accounts)
            
            # Incoming to transit
            transactions.append({
                "tx_id": str(uuid.uuid4()),
                "operation_date": base_date.date(),
                "operation_ts": base_date,
                "amount_kzt": random.choice([500000, 1000000, 2000000]),
                "direction": "credit",
                "payer_name": random.choice(payers),
                "receiver_name": transit_account,
                "purpose_text": "Поступление",
                "operation_type_raw": "Transit In",
                "source_bank": "HALYK",
                "row_hash": str(uuid.uuid4()),
                "semantic_text": "transit транзит intermediate промежуточный"
            })
            
            # Outgoing from transit
            transactions.append({
                "tx_id": str(uuid.uuid4()),
                "operation_date": (base_date + timedelta(hours=random.randint(1, 24))).date(),
                "operation_ts": base_date + timedelta(hours=random.randint(1, 24)),
                "amount_kzt": random.choice([450000, 950000, 1950000]),
                "direction": "debit",
                "payer_name": transit_account,
                "receiver_name": random.choice(receivers),
                "purpose_text": "Выплата",
                "operation_type_raw": "Transit Out",
                "source_bank": "HALYK",
                "row_hash": str(uuid.uuid4()),
                "semantic_text": "transit транзит intermediate промежуточный"
            })
        
        return transactions

    @staticmethod
    def generate_missing_purpose_transactions(count: int = 30) -> List[Dict[str, Any]]:
        """Generate suspicious transactions with missing purpose."""
        transactions = []
        payers = TransactionGenerator.FAKE_NAMES["payers"]
        receivers = TransactionGenerator.FAKE_NAMES["receivers"]

        for i in range(count):
            base_date = datetime.now() - timedelta(days=random.randint(1, 180))
            
            transactions.append({
                "tx_id": str(uuid.uuid4()),
                "operation_date": base_date.date(),
                "operation_ts": base_date,
                "amount_kzt": random.choice(TransactionGenerator.ROUND_AMOUNTS),
                "direction": random.choice(["debit", "credit"]),
                "payer_name": random.choice(payers),
                "receiver_name": random.choice(receivers),
                "purpose_text": None,  # Missing purpose
                "operation_type_raw": "Unknown",
                "source_bank": random.choice(["HALYK", "KASPI"]),
                "row_hash": str(uuid.uuid4()),
                "semantic_text": "unknown без назначения"
            })
        
        return transactions

    @staticmethod
    def generate_all(sizes: Dict[str, int] = None) -> List[Dict[str, Any]]:
        """Generate all types of transactions."""
        if sizes is None:
            sizes = {
                "circular": 20,
                "real_estate": 50,
                "ip_entrepreneur": 30,
                "cash_out": 100,
                "rapid_fire": 50,
                "transit": 40,
                "missing_purpose": 30
            }
        
        all_transactions = []
        all_transactions.extend(TransactionGenerator.generate_circular_transactions(sizes["circular"]))
        all_transactions.extend(TransactionGenerator.generate_real_estate_transactions(sizes["real_estate"]))
        all_transactions.extend(TransactionGenerator.generate_ip_entrepreneur_transactions(sizes["ip_entrepreneur"]))
        all_transactions.extend(TransactionGenerator.generate_cash_out_transactions(sizes["cash_out"]))
        all_transactions.extend(TransactionGenerator.generate_rapid_fire_transactions(sizes["rapid_fire"]))
        all_transactions.extend(TransactionGenerator.generate_transit_accounts(sizes["transit"]))
        all_transactions.extend(TransactionGenerator.generate_missing_purpose_transactions(sizes["missing_purpose"]))
        
        return all_transactions


def insert_test_data(db_session, transaction_data: List[Dict[str, Any]]) -> int:
    """
    Insert generated test transactions into database.
    
    Args:
        db_session: SQLAlchemy session
        transaction_data: List of transaction dicts
    
    Returns:
        Number of inserted transactions
    """
    from app.db.schema import Transaction
    
    count = 0
    for tx_dict in transaction_data:
        tx = Transaction(**tx_dict)
        db_session.add(tx)
        count += 1
    
    db_session.commit()
    return count


if __name__ == "__main__":
    import sys
    
    print("💾 Financial Crime Test Data Generator")
    print("=" * 50)
    
    # Generate sample data
    data = TransactionGenerator.generate_all()
    
    print(f"✅ Generated {len(data)} test transactions:")
    print(f"   - Circular: 20")
    print(f"   - Real Estate: 50")
    print(f"   - IP Entrepreneurs: 30")
    print(f"   - Cash Out: 100")
    print(f"   - Rapid Fire: 50")
    print(f"   - Transit: 40")
    print(f"   - Missing Purpose: 30")
    print()
    print("To insert into database, use:")
    print("  python -c 'from scripts.generate_test_data import *; ...'")
