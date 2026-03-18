# Advanced Financial Crime Detection - Complete Guide

## Overview

This document covers the advanced fraud detection capabilities added to the AFM system:

1. **Advanced Query Templates** - Pre-built SQL patterns for financial crime detection
2. **Fraud Detection Patterns** - Statistical and behavioral analysis modules
3. **Test Data Generator** - Realistic transaction data for validation
4. **Advanced CLI Tool** - Command-line interface for templates and fraud analysis

---

## 1. Advanced Query Templates

### Module: `app/nl2sql/advanced_templates.py`

Pre-built SQL templates for complex financial crime detection queries.

#### Available Templates

| Template | Purpose | Key SQL Pattern |
|----------|---------|-----------------|
| `circular_transactions` | Self-transfers (самопереводы) | `WHERE payer_name = receiver_name` |
| `real_estate_transactions` | Real estate payments | `WHERE LOWER(purpose_text) LIKE '%недвижим%'` |
| `ip_entrepreneur_transactions` | ИП payments with aggregation | `WHERE receiver_name LIKE '%ип%' GROUP BY receiver_name` |
| `cash_out_obnal` | Obnal/cash-out schemes | `WHERE direction = 'debit' GROUP BY receiver_name` |
| `round_amount_transactions` | Suspicious round amounts | `WHERE amount_kzt IN (1M, 500K, 250K, 100K, ...)` |
| `rapid_fire_transactions` | Rapid sequential patterns | Time-window based GROUP BY |
| `transit_accounts` | Intermediary accounts | Using INNER JOIN on payer/receiver |
| `suspicious_patterns_summary` | Overall pattern summary | Multi-CASE aggregation |
| `high_value_to_ip` | Large payments to ИП | `WHERE amount_kzt >= limit AND receiver LIKE '%ип%'` |
| `repeated_payer_receiver_pairs` | Recurring patterns | `GROUP BY payer, receiver HAVING COUNT(*) >= 5` |
| `missing_purpose_transactions` | High-risk missing data | `WHERE purpose_text IS NULL OR LENGTH < 5` |
| `pattern_by_bank` | Bank-specific patterns | Parameterized by source_bank |

#### Usage Examples

```python
from app.nl2sql.advanced_templates import AdvancedQueryTemplates

templates = AdvancedQueryTemplates()

# Get circular transaction SQL
sql = templates.circular_transactions()

# Get parameterized template
sql = templates.cash_out_obnal(min_amount=500000)

# List all templates
from app.nl2sql.advanced_templates import list_templates
all_templates = list_templates()
```

#### Template Structure

Each template follows this pattern:

```python
@staticmethod
def template_name() -> str:
    """Description of what it detects."""
    return """
    SELECT col1, col2, ...
    FROM afm.transactions_nl_view
    WHERE [conditions]
    LIMIT 100;
    """
```

---

## 2. Fraud Detection Patterns

### Module: `app/nl2sql/fraud_patterns.py`

Comprehensive fraud detection using statistical and behavioral analysis.

### Classes

#### `AnomalyDetector`
Statistical anomaly detection for individual transactions.

**Methods:**

```python
# Detect if amount deviates from historical pattern
is_anomalous, z_score = AnomalyDetector.detect_amount_anomaly(
    transaction_amount=500000,
    historical_amounts=[100000, 150000, 120000, ...],
    threshold_std=2.0  # 2 sigma = 95% confidence
)

# Detect rapid-fire transaction sequences
tx_count, anomaly_score = AnomalyDetector.detect_frequency_anomaly(
    tx_timestamps=[...],
    window_minutes=60
)

# Detect round amount patterns (obnal indicator)
is_round, label = AnomalyDetector.detect_round_amount_pattern(500000)
# Returns: (True, '500K')

# Detect unusual counterparty relationships
is_anomalous, score = AnomalyDetector.detect_counterparty_anomaly(
    payer="Entity A",
    receiver="Entity B",
    historical_counterparties=[...]
)
```

#### `BehavioralAnalyzer`
Behavioral pattern analysis for entities.

**Methods:**

```python
# Analyze entity risk profile
profile = BehavioralAnalyzer.analyze_entity_behavior(
    entity_name="ТОО EXAMPLE",
    transactions=[{...}, {...}, ...]
)
# Returns: EntityRiskProfile with:
# - risk_score (0-100)
# - total_transactions
# - total_amount
# - suspicious_indicators (list)
# - last_activity (datetime)

# Detect behavior changes
changed, score = BehavioralAnalyzer.detect_behavior_change(
    historical_profile=old_profile,
    current_profile=new_profile
)
```

**Behavioral Indicators Checked:**

1. **High Average Amount** - Avg > 1M KZT = +40 points
2. **Round Amount Pattern** - >70% round amounts = +35 points
3. **High Frequency** - >20 transactions = +25 points
4. **Repetitive Patterns** - Low diversity of purposes = +20 points
5. **Missing Purpose** - >30% missing purpose = +30 points

#### `SchemeDetector`
Detection of known fraud schemes.

**Methods:**

```python
# Detect self-transfers
is_circular = SchemeDetector.detect_circular_scheme(
    payer="ТОО COMPANY",
    receiver="ТОО COMPANY"
)

# Detect layering chains (A→B→C→D)
is_layering, chains = SchemeDetector.detect_layering_pattern(
    transactions=[...],
    max_depth=5
)

# Detect obnal (cash out) schemes
is_obnal, score = SchemeDetector.detect_obnal_scheme(
    transactions=[...],
    min_total_amount=500000
)

# Detect real estate anomalies
is_re_anomaly, score = SchemeDetector.detect_real_estate_anomaly(
    transactions=[...]
)
```

**Obnal Detection Indicators:**

- High debit ratio (>70%) = +20 points
- Predominantly round amounts (>50%) = +25 points
- Missing purpose (>30%) = +20 points
- High frequency (>15 txs) = +15 points
- Total >= threshold = +10 points

#### `RiskScorer`
Combined risk scoring for transactions and entities.

```python
# Score single transaction
risk_score = RiskScorer.calculate_transaction_risk(
    transaction={...},
    historical_data={...}  # Optional
)

# Score entity
entity_risk = RiskScorer.calculate_entity_risk(profile)
```

**Risk Calculation Components:**

- Amount anomaly: up to 40 points
- Round amount: +15 points
- Self-transfer: +40 points
- Missing purpose: +20 points
- Debit direction: +10 points

### Data Classes

#### `TransactionAnomaly`

```python
@dataclass
class TransactionAnomaly:
    tx_id: str
    anomaly_type: str  # "amount", "frequency", "counterparty", "timing"
    score: float      # 0-100
    reason: str
    severity: str     # "LOW", "MEDIUM", "HIGH", "CRITICAL"
```

#### `EntityRiskProfile`

```python
@dataclass
class EntityRiskProfile:
    entity_name: str
    risk_score: float           # 0-100
    total_transactions: int
    total_amount: float
    avg_amount: float
    suspicious_indicators: List[str]
    last_activity: datetime
```

---

## 3. Test Data Generator

### Module: `scripts/generate_test_data.py`

Generate realistic financial transaction data for testing fraud detection patterns.

### Usage

```python
from scripts.generate_test_data import TransactionGenerator

# Generate all types
all_data = TransactionGenerator.generate_all()

# Or specific types
circular = TransactionGenerator.generate_circular_transactions(count=20)
real_estate = TransactionGenerator.generate_real_estate_transactions(count=50)
ip_txs = TransactionGenerator.generate_ip_entrepreneur_transactions(count=30)
cash_out = TransactionGenerator.generate_cash_out_transactions(count=100)
rapid_fire = TransactionGenerator.generate_rapid_fire_transactions(count=50)
transit = TransactionGenerator.generate_transit_accounts(count=40)
missing = TransactionGenerator.generate_missing_purpose_transactions(count=30)
```

### Data Types Generated

#### 1. Circular Transactions (20 records)
- **What:** Self-transfers (payer_name = receiver_name)
- **Risk:** High - indicates potential fraud or money laundering
- **Amounts:** 50K-500K KZT
- **Purpose:** Suspicious/missing

#### 2. Real Estate Transactions (50 records)
- **What:** Payments with "недвижимость", "квартира" in purpose
- **Risk:** Medium-High - legitimate but requires scrutiny
- **Amounts:** 1M-10M KZT
- **Receivers:** Real estate companies

#### 3. IP Entrepreneur Transactions (30 records)
- **What:** Payments to individuals/IPs
- **Risk:** Medium - legitimate but monitoring needed
- **Amounts:** 100K-2M KZT
- **Pattern:** Various payers to same ИП

#### 4. Cash Out / Obnal Transactions (100 records)
- **What:** Debit-heavy with round amounts
- **Risk:** High - classic obnal pattern
- **Amounts:** Mix of round amounts (1M, 500K, 250K, 100K, etc.)
- **Purpose:** Missing/suspicious

#### 5. Rapid Fire Transactions (50 records)
- **What:** Sequential txs from same entity within 1 hour
- **Risk:** Medium-High - money laundering indicator
- **Pattern:** Same payer to different receivers
- **Timing:** All within 1-hour window

#### 6. Transit Accounts (40 records)
- **What:** Accounts appearing as both payer and receiver
- **Risk:** High - potential layering
- **Pattern:** A→Transit→B within 1-24 hours
- **Amounts:** Slight reduction (fee-like pattern)

#### 7. Missing Purpose Transactions (30 records)
- **What:** NULL or very short purpose_text
- **Risk:** High - documentation gap
- **Amounts:** Round amounts
- **Pattern:** No transaction description

### Transaction Schema

Generated transactions follow this structure:

```python
{
    "tx_id": str,                    # UUID
    "operation_date": date,
    "operation_ts": datetime,
    "amount_kzt": float,
    "direction": str,                # "debit" or "credit"
    "payer_name": str,
    "receiver_name": str,
    "purpose_text": str or None,
    "operation_type_raw": str,
    "source_bank": str,              # "HALYK", "KASPI", "EURASYA"
    "row_hash": str,                 # UUID for deduplication
    "semantic_text": str             # For semantic search
}
```

### Insertion into Database

```python
from app.db.engine import get_session
from scripts.generate_test_data import TransactionGenerator, insert_test_data

session = get_session()
data = TransactionGenerator.generate_all()
count = insert_test_data(session, data)
print(f"Inserted {count} transactions")
```

---

## 4. Advanced CLI Tool

### Script: `scripts/advanced_cli.py`

Comprehensive command-line interface for templates and fraud analysis.

### Commands Overview

| Category | Command | Purpose |
|----------|---------|---------|
| **Templates** | `list` | List all query templates |
| | `describe <name>` | Show template details |
| | `sql <name>` | Print SQL code |
| | `run <name>` | Execute template |
| **Fraud** | `analyze-tx <id>` | Analyze single transaction |
| | `analyze-entity <name>` | Analyze entity risk profile |
| | `patterns <type>` | Find specific fraud patterns |
| **Test Data** | `generate` | Generate test data |
| | `insert` | Insert test data into DB |

### Usage Examples

#### Templates Commands

```bash
# List all available templates
python scripts/advanced_cli.py templates list

# Get template details
python scripts/advanced_cli.py templates describe circular_transactions

# View SQL for template
python scripts/advanced_cli.py templates sql cash_out_obnal

# Execute template and get results
python scripts/advanced_cli.py templates run circular_transactions
```

#### Fraud Analysis Commands

```bash
# Analyze specific transaction
python scripts/advanced_cli.py fraud analyze-tx "123e4567-e89b-12d3-a456-426614174000"

# Analyze entity risk profile
python scripts/advanced_cli.py fraud analyze-entity "ТОО SMILEFACE"

# Find specific fraud patterns
python scripts/advanced_cli.py fraud patterns circular         # Self-transfers
python scripts/advanced_cli.py fraud patterns round_amounts   # Round amounts
python scripts/advanced_cli.py fraud patterns missing_purpose # Missing docs
python scripts/advanced_cli.py fraud patterns debit_heavy     # Heavy debit entities
```

#### Test Data Commands

```bash
# Generate test fraud data
python scripts/advanced_cli.py test-data generate

# Insert into database
python scripts/advanced_cli.py test-data insert
```

### Output Examples

#### Template List Output
```
============================================================
  Available Query Templates
============================================================

 1. circular_transactions
    Find transfers where payer sends to themselves

 2. real_estate_transactions
    Find real estate related payments

 3. ip_entrepreneur_transactions
    Find transactions with individual entrepreneurs

...
```

#### Transaction Analysis Output
```
============================================================
  Transaction Risk Analysis: 123e4567...
============================================================

Transaction Details:
  Date: 2024-03-15
  Amount: 500,000.00 KZT
  Direction: debit
  From: ТОО COMPANY A
  To: ТОО COMPANY B
  Purpose: (missing)

Risk Indicators:
  Round Amount: ✅ YES (500K)
  Self-Transfer: ❌ NO
  Missing Purpose: ✅ YES
  Debit Direction: ✅ YES

Overall Risk: 🟠 HIGH
Risk Score: 65.0/100
```

#### Entity Analysis Output
```
============================================================
  Entity Risk Profile: ТОО EXAMPLE
============================================================

Entity Profile:
  Name: ТОО EXAMPLE
  Total Transactions: 45
  Total Amount: 15,250,000.00 KZT
  Average Amount: 338,888.89 KZT
  Last Activity: 2024-03-20 14:30

Suspicious Indicators:
  ⚠️  High average transaction amount
  ⚠️  Predominantly round amounts
  ⚠️  High transaction frequency

Risk Profile: 🟠 HIGH
Risk Score: 72.5/100
```

---

## 5. Integration Points

### With QueryService

```python
from app.nl2sql.query_service import QueryService
from app.nl2sql.advanced_templates import AdvancedQueryTemplates

service = QueryService()
templates = AdvancedQueryTemplates()

# Get template SQL
sql = templates.circular_transactions()

# Execute via QueryService
result = service.run_raw_sql(sql)
```

### With Prompt Builder

The prompt builder now includes advanced patterns section:

```
## ADVANCED PATTERNS FOR FINANCIAL CRIMES

### CIRCULAR TRANSACTIONS
WHERE payer_name = receiver_name AND payer_name IS NOT NULL

### REAL ESTATE TRANSACTIONS
WHERE LOWER(COALESCE(purpose_text, '')) LIKE '%недвижим%'

### IP ENTREPRENEURS
WHERE receiver_name LIKE '%ип%' AND receiver_name IS NOT NULL

### OBNAL / CASH OUT
WHERE direction = 'debit' AND receiver_name IS NOT NULL GROUP BY receiver_name

### SUSPICIOUS PATTERNS
WHERE amount_kzt IN (1000000, 500000, 250000, 100000) AND direction = 'debit'
```

### With Entity Extractor

Added semantic keywords for fraud detection:

```python
"real_estate": r"недвижим|real estate|квартир|дом|property"
"ip_entrepreneur": r"ип|индивидуальный предприниматель"
"cash_out": r"обнал|обналичи|cash_out"
"circular": r"круговой|круговые|circular"
"transit": r"транзит|transit|промежуточный"
```

---

## 6. Real-World Examples

### Example 1: Detecting Obnal Scheme

```python
from app.nl2sql.fraud_patterns import SchemeDetector
from app.db.engine import get_session
from app.db.schema import Transaction

session = get_session()

# Get all debits for an entity
entity_txs = session.query(Transaction).filter(
    (Transaction.payer_name == "ТОО SUSPECT") &
    (Transaction.direction == "debit")
).all()

tx_dicts = [{
    "amount_kzt": tx.amount_kzt,
    "direction": tx.direction,
    "payer_name": tx.payer_name,
    "receiver_name": tx.receiver_name,
    "purpose_text": tx.purpose_text,
    "operation_ts": tx.operation_ts,
    "operation_type_raw": tx.operation_type_raw,
} for tx in entity_txs]

# Detect obnal
is_obnal, score = SchemeDetector.detect_obnal_scheme(tx_dicts, min_total_amount=500000)

if is_obnal and score > 70:
    print(f"🚨 High-risk obnal detected! Score: {score}")
```

### Example 2: Entity Risk Monitoring

```python
from app.nl2sql.fraud_patterns import BehavioralAnalyzer, EntityRiskProfile

# Analyze entity
profile = BehavioralAnalyzer.analyze_entity_behavior("ТОО COMPANY", transactions)

# Check risk level
if profile.risk_score >= 80:
    print("🔴 CRITICAL RISK")
    for indicator in profile.suspicious_indicators:
        print(f"  - {indicator}")
elif profile.risk_score >= 60:
    print("🟠 HIGH RISK - Requires investigation")
```

### Example 3: Using Templates in NL Query

User query: "Найди все самопереводы"
System:
1. Recognizes "самопереводы" (circular) pattern
2. Looks up `circular_transactions` template
3. Returns pre-optimized SQL
4. Executes and displays results

---

## 7. Best Practices

### For Fraud Analysts

1. **Start with Templates** - Use pre-built templates for common patterns
2. **Combine Multiple Indicators** - Single indicator not conclusive
3. **Monitor Behavior Changes** - Sudden shifts in patterns are suspicious
4. **Check Context** - Risk score in context of business type
5. **Verify Findings** - Manual review before escalation

### For Developers

1. **Graceful Degradation** - Fraud detection is optional, not blocking
2. **Semantic Enrichment** - Use semantic_text field for embeddings
3. **Performance** - Test templates with large datasets
4. **Error Handling** - Handle missing or NULL data appropriately
5. **Documentation** - Document custom fraud patterns

### For Database

1. **Index Key Fields** - Ensure indexes on:
   - `payer_name`, `receiver_name` (for joins)
   - `amount_kzt` (for range queries)
   - `direction` (for filtering)
   - `operation_date` (for time-based analysis)

2. **Partitioning** - Consider date-based partitioning for large tables

3. **Archive Strategy** - Archive old transactions for performance

---

## 8. Configuration

### Thresholds and Parameters

Edit as needed:

```python
# Anomaly detection
AMOUNT_ANOMALY_THRESHOLD = 2.0  # Standard deviations
FREQUENCY_ANOMALY_WINDOW = 60   # Minutes

# Risk scoring
CIRCULAR_TRANSFER_SCORE = 40
MISSING_PURPOSE_SCORE = 20
ROUND_AMOUNT_SCORE = 15

# Pattern detection
MIN_OBNAL_TRANSACTIONS = 5
MIN_OBNAL_AMOUNT = 500000
ROUND_AMOUNTS = [1000000, 500000, 250000, 100000, ...]
```

---

## 9. Troubleshooting

### Issue: No patterns detected

**Causes:**
- Test data not inserted
- Thresholds too strict
- Database indexes missing

**Solutions:**
1. Run `test-data insert` to populate
2. Check threshold values
3. Run `ANALYZE` on table to update statistics

### Issue: Slow template execution

**Causes:**
- Missing indexes
- Complex joins
- Large dataset

**Solutions:**
1. Add indexes on payer_name, receiver_name
2. Use LIMIT in templates
3. Consider date-based filtering

### Issue: False positives in fraud detection

**Causes:**
- Legitimate business patterns flagged
- Thresholds too low
- Single indicator weighted too high

**Solutions:**
1. Increase thresholds for business type
2. Require multiple indicators (AND not OR)
3. White-list known legitimate entities

---

## 10. Future Enhancements

1. **Machine Learning Models** - Train on historical fraud data
2. **Graph Analysis** - Network analysis of transaction flows
3. **Time Series Analysis** - Seasonal pattern detection
4. **Real-time Monitoring** - Stream processing alerts
5. **Custom Fraud Rules** - User-defined pattern engine
6. **Visualization Dashboard** - Risk heatmaps and networks

---

## Summary

The AFM system now provides comprehensive tools for financial crime detection:

✅ **12 Pre-built Templates** - Instant pattern detection
✅ **4 Analysis Classes** - Statistical, behavioral, scheme-based, risk scoring
✅ **Test Data Generator** - 320 realistic fraud transactions
✅ **Advanced CLI** - Easy-to-use command interface
✅ **Integration Ready** - Works with existing NL2SQL pipeline

**Total Addition:** ~1500 lines of production-ready code across 4 files.
