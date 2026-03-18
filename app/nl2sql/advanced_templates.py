from __future__ import annotations

"""
Advanced Query Templates

Pre-built SQL templates for complex financial crime detection and analytics.
Each method returns a ready-to-execute SQL string against afm.transactions_nl_view.

Supported patterns:
  ── Fraud detection ─────────────────────────────────────────────────────────
  circular_transactions()          Self-transfers (A→A)
  circular_transactions_2hop()     True circular flow (A→B→A)
  transit_accounts()               Entities acting as pass-through intermediaries
  obnal_by_type()                  Cash-out grouped by operation type
  obnal_total(year)                Total debit outflow, optionally filtered by year
  ip_suspicious_schemes()          IP entrepreneurs with high-volume suspicious patterns
  real_estate_transactions()       Real estate related payments
  suspicious_patterns_summary()    One-row dashboard of all suspicious indicators
  round_amount_transactions()      Transactions with round amounts (structuring signal)
  rapid_fire_transactions(hours)   Multiple transactions in short time window
  missing_purpose_transactions()   Transactions with empty/minimal purpose text
  repeated_payer_receiver_pairs()  Same entity pairs transacting repeatedly

  ── Analytics ───────────────────────────────────────────────────────────────
  top_banks_by_withdrawal()        Banks ranked by total debit volume
  client_incoming(client_name)     All credit transactions for a specific client
  period_summary(from_date, to_date)  Monthly aggregation of credit/debit by period
  high_value_to_ip(min_amount)     Large payments to individual entrepreneurs
  pattern_by_bank(source_bank)     Suspicious patterns for a specific bank
  cash_out_obnal(min_amount)       Debit aggregation by receiver (legacy)
  ip_entrepreneur_transactions()   IP transactions grouped by receiver (legacy)
"""

from typing import Any, Dict, List, Optional

_VIEW = "afm.transactions_nl_view"


class AdvancedQueryTemplates:
    """Pre-built templates for advanced financial analysis."""

    # ── Fraud: circular transactions ─────────────────────────────────────────

    @staticmethod
    def circular_transactions() -> str:
        """Self-transfers: same entity sends to itself (payer = receiver)."""
        return f"""
SELECT tx_id, operation_date, amount_kzt, direction,
       payer_name, receiver_name, purpose_text
FROM {_VIEW}
WHERE payer_name = receiver_name
  AND payer_name IS NOT NULL
ORDER BY operation_date DESC
LIMIT 100;
"""

    @staticmethod
    def circular_transactions_2hop(days: int = 30) -> str:
        """
        True 2-hop circular flow: A sends to B, B sends back to A within <days>.
        Excludes simple self-transfers (handled by circular_transactions).
        """
        return f"""
SELECT
    t1.tx_id           AS tx1_id,
    t2.tx_id           AS tx2_id,
    t1.payer_name      AS entity_a,
    t1.receiver_name   AS entity_b,
    t1.amount_kzt      AS a_to_b,
    t2.amount_kzt      AS b_to_a,
    t1.operation_date  AS date_a_to_b,
    t2.operation_date  AS date_b_to_a,
    t1.purpose_text    AS purpose_1,
    t2.purpose_text    AS purpose_2
FROM {_VIEW} t1
JOIN {_VIEW} t2
  ON t1.receiver_name  = t2.payer_name
 AND t2.receiver_name  = t1.payer_name
 AND t1.tx_id         <> t2.tx_id
 AND t1.payer_name    <> t1.receiver_name
 AND t2.operation_date BETWEEN t1.operation_date
                            AND t1.operation_date + INTERVAL '{days} days'
WHERE t1.payer_name IS NOT NULL
  AND t1.receiver_name IS NOT NULL
ORDER BY t1.operation_date DESC
LIMIT 100;
"""

    # ── Fraud: transit accounts ───────────────────────────────────────────────

    @staticmethod
    def transit_accounts() -> str:
        """
        Transit (pass-through) accounts: entities that appear as BOTH payer
        and receiver with a high in≈out passthrough ratio (≥70 %).
        Sorted by passthrough_pct DESC, then by total inflow.
        """
        return f"""
WITH inflow AS (
    SELECT
        receiver_name            AS entity,
        COUNT(*)                 AS cnt_in,
        SUM(amount_kzt)          AS total_in
    FROM {_VIEW}
    WHERE receiver_name IS NOT NULL
    GROUP BY receiver_name
),
outflow AS (
    SELECT
        payer_name               AS entity,
        COUNT(*)                 AS cnt_out,
        SUM(amount_kzt)          AS total_out
    FROM {_VIEW}
    WHERE payer_name IS NOT NULL
    GROUP BY payer_name
)
SELECT
    i.entity,
    i.cnt_in,
    o.cnt_out,
    ROUND(i.total_in::numeric,  2) AS total_in,
    ROUND(o.total_out::numeric, 2) AS total_out,
    ROUND(
        (LEAST(i.total_in, o.total_out)
         / NULLIF(GREATEST(i.total_in, o.total_out), 0) * 100)::numeric,
        1
    ) AS passthrough_pct
FROM inflow  i
JOIN outflow o ON i.entity = o.entity
WHERE i.cnt_in  >= 3
  AND o.cnt_out >= 3
  AND LEAST(i.total_in, o.total_out)
      / NULLIF(GREATEST(i.total_in, o.total_out), 0) > 0.7
ORDER BY passthrough_pct DESC, i.total_in DESC
LIMIT 50;
"""

    # ── Fraud: obnal (cash-out) ───────────────────────────────────────────────

    @staticmethod
    def obnal_by_type() -> str:
        """Cash-out (debit) aggregated by operation type — shows which channels are used."""
        return f"""
SELECT
    operation_type_raw,
    COUNT(*)                         AS tx_count,
    SUM(amount_kzt)                  AS total_amount,
    ROUND(AVG(amount_kzt)::numeric, 2) AS avg_amount,
    COUNT(DISTINCT receiver_name)    AS unique_receivers,
    MIN(operation_date)              AS first_date,
    MAX(operation_date)              AS last_date
FROM {_VIEW}
WHERE direction = 'debit'
  AND operation_type_raw IS NOT NULL
GROUP BY operation_type_raw
ORDER BY total_amount DESC
LIMIT 30;
"""

    @staticmethod
    def obnal_total(year: Optional[int] = None) -> str:
        """
        Total debit outflow, optionally filtered by year.
        Groups by year + operation type to show obnal breakdown.
        """
        year_filter = ""
        if year:
            year_filter = f"  AND operation_date BETWEEN '{year}-01-01' AND '{year}-12-31'"

        return f"""
SELECT
    EXTRACT(YEAR FROM operation_date)::int AS year,
    operation_type_raw,
    COUNT(*)                               AS tx_count,
    SUM(amount_kzt)                        AS total_obnal,
    ROUND(AVG(amount_kzt)::numeric, 2)     AS avg_per_tx,
    COUNT(DISTINCT receiver_name)          AS unique_receivers
FROM {_VIEW}
WHERE direction = 'debit'
  AND operation_date IS NOT NULL
{year_filter}
GROUP BY year, operation_type_raw
ORDER BY total_obnal DESC;
"""

    # ── Fraud: IP entrepreneur schemes ───────────────────────────────────────

    @staticmethod
    def ip_suspicious_schemes(
        min_amount: float = 1_000_000,
        min_tx: int = 5,
    ) -> str:
        """
        IP entrepreneurs receiving large aggregate amounts from many payers.
        High unique_payers + high total_amount = shell-IP scheme indicator.
        """
        return f"""
SELECT
    receiver_name,
    COUNT(*)                               AS tx_count,
    SUM(amount_kzt)                        AS total_amount,
    ROUND(AVG(amount_kzt)::numeric, 2)     AS avg_amount,
    COUNT(DISTINCT payer_name)             AS unique_payers,
    MIN(operation_date)                    AS first_tx,
    MAX(operation_date)                    AS last_tx,
    MAX(operation_date) - MIN(operation_date) AS active_days
FROM {_VIEW}
WHERE (
    LOWER(COALESCE(receiver_name, '')) LIKE '%ип %'
    OR LOWER(COALESCE(receiver_name, '')) LIKE '% ип%'
    OR LOWER(COALESCE(receiver_name, '')) LIKE '%индивидуальный%'
    OR LOWER(COALESCE(receiver_name, '')) LIKE '%ип«%'
    OR LOWER(COALESCE(receiver_name, '')) LIKE '%ип"%'
)
  AND receiver_name IS NOT NULL
GROUP BY receiver_name
HAVING COUNT(*) >= {min_tx}
   AND SUM(amount_kzt) > {min_amount}
ORDER BY total_amount DESC
LIMIT 30;
"""

    @staticmethod
    def ip_entrepreneur_transactions() -> str:
        """Find transactions with individual entrepreneurs (ИП) — legacy aggregate."""
        return f"""
SELECT receiver_name,
       SUM(amount_kzt) AS total_amount,
       COUNT(*)        AS tx_count,
       AVG(amount_kzt) AS avg_amount,
       MIN(amount_kzt) AS min_amount,
       MAX(amount_kzt) AS max_amount
FROM {_VIEW}
WHERE (receiver_name LIKE '%ип%'
    OR receiver_name LIKE '%индивидуальный%'
    OR receiver_name LIKE '%ИП %')
  AND receiver_name IS NOT NULL
GROUP BY receiver_name
ORDER BY total_amount DESC
LIMIT 20;
"""

    # ── Fraud: real estate ────────────────────────────────────────────────────

    @staticmethod
    def real_estate_transactions() -> str:
        """Find real estate related transactions by purpose_text keywords."""
        return f"""
SELECT
    tx_id, operation_date, amount_kzt, direction,
    payer_name, receiver_name, purpose_text
FROM {_VIEW}
WHERE (
    LOWER(COALESCE(purpose_text, '')) LIKE '%недвижим%'
    OR LOWER(COALESCE(purpose_text, '')) LIKE '%квартир%'
    OR LOWER(COALESCE(purpose_text, '')) LIKE '%дом%'
    OR LOWER(COALESCE(purpose_text, '')) LIKE '%участок%'
    OR LOWER(COALESCE(purpose_text, '')) LIKE '%нежилое%'
    OR LOWER(COALESCE(raw_note,     '')) LIKE '%недвижим%'
    OR LOWER(COALESCE(raw_note,     '')) LIKE '%квартир%'
)
ORDER BY amount_kzt DESC
LIMIT 100;
"""

    # ── Analytics ─────────────────────────────────────────────────────────────

    @staticmethod
    def top_banks_by_withdrawal() -> str:
        """Banks ranked by total debit (withdrawal) volume."""
        return f"""
SELECT
    source_bank,
    COUNT(*)                               AS tx_count,
    SUM(amount_kzt)                        AS total_withdrawal,
    ROUND(AVG(amount_kzt)::numeric, 2)     AS avg_withdrawal,
    MAX(amount_kzt)                        AS max_single
FROM {_VIEW}
WHERE direction = 'debit'
  AND source_bank IS NOT NULL
GROUP BY source_bank
ORDER BY total_withdrawal DESC
LIMIT 20;
"""

    @staticmethod
    def client_incoming(client_name: Optional[str] = None) -> str:
        """
        All credit (incoming) transactions.
        Optionally filtered to a specific client name (ILIKE on client_name / receiver_name).
        """
        client_filter = ""
        if client_name:
            safe = client_name.replace("'", "''")
            client_filter = f"""  AND (
    LOWER(COALESCE(client_name,   '')) ILIKE '%{safe.lower()}%'
    OR LOWER(COALESCE(receiver_name, '')) ILIKE '%{safe.lower()}%'
  )"""

        return f"""
SELECT
    tx_id, operation_date, amount_kzt,
    payer_name, client_name, receiver_name,
    purpose_text, source_bank
FROM {_VIEW}
WHERE direction = 'credit'
{client_filter}
ORDER BY operation_date DESC
LIMIT 100;
"""

    @staticmethod
    def period_summary(
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> str:
        """
        Monthly aggregation of credit vs debit totals.
        If from_date / to_date provided (ISO format), limits to that range.
        """
        date_filter = ""
        if from_date and to_date:
            date_filter = f"WHERE operation_date BETWEEN '{from_date}' AND '{to_date}'"
        elif from_date:
            date_filter = f"WHERE operation_date >= '{from_date}'"
        elif to_date:
            date_filter = f"WHERE operation_date <= '{to_date}'"

        return f"""
SELECT
    TO_CHAR(operation_date, 'YYYY-MM') AS period,
    SUM(CASE WHEN direction = 'credit' THEN amount_kzt ELSE 0 END) AS total_credit,
    SUM(CASE WHEN direction = 'debit'  THEN amount_kzt ELSE 0 END) AS total_debit,
    COUNT(*) AS tx_count,
    COUNT(DISTINCT payer_name)    AS unique_payers,
    COUNT(DISTINCT receiver_name) AS unique_receivers
FROM {_VIEW}
{date_filter}
GROUP BY period
ORDER BY period;
"""

    # ── Other fraud patterns ──────────────────────────────────────────────────

    @staticmethod
    def round_amount_transactions() -> str:
        """Suspicious round-amount debit transactions (structuring / obnal signal)."""
        return f"""
SELECT tx_id, operation_date, amount_kzt, direction,
       payer_name, receiver_name, purpose_text, operation_type_raw
FROM {_VIEW}
WHERE amount_kzt IN (
    1000000, 900000, 800000, 700000, 600000,
    500000,  400000, 300000, 250000,
    200000,  150000, 100000,
    75000,   50000,  25000,  10000
)
  AND direction = 'debit'
ORDER BY operation_date DESC
LIMIT 100;
"""

    @staticmethod
    def rapid_fire_transactions(time_window_hours: int = 1) -> str:
        """
        Entities sending ≥3 transactions within a short window —
        potential layering / money-laundering signal.
        """
        return f"""
WITH tx_groups AS (
    SELECT
        payer_name,
        COUNT(*)                       AS tx_in_window,
        SUM(amount_kzt)                AS total_in_window,
        COUNT(DISTINCT receiver_name)  AS distinct_receivers,
        MIN(operation_ts)              AS first_tx,
        MAX(operation_ts)              AS last_tx
    FROM {_VIEW}
    WHERE operation_ts >= NOW() - INTERVAL '{time_window_hours} hours'
      AND payer_name IS NOT NULL
    GROUP BY payer_name
    HAVING COUNT(*) >= 3
)
SELECT payer_name, tx_in_window, distinct_receivers,
       total_in_window, first_tx, last_tx
FROM tx_groups
ORDER BY tx_in_window DESC, total_in_window DESC
LIMIT 50;
"""

    @staticmethod
    def suspicious_patterns_summary() -> str:
        """One-row dashboard: counts of key suspicious indicators across all data."""
        return f"""
SELECT
    COUNT(DISTINCT CASE WHEN payer_name = receiver_name
                        THEN tx_id END)                    AS self_transfer_count,
    COUNT(DISTINCT CASE WHEN amount_kzt IN
                         (1000000,500000,250000,100000,50000)
                        AND direction = 'debit'
                        THEN tx_id END)                    AS round_amount_debit_count,
    COUNT(DISTINCT CASE WHEN direction = 'debit'
                        THEN tx_id END)                    AS total_debit_tx,
    SUM(CASE WHEN direction = 'debit'
             THEN amount_kzt ELSE 0 END)                  AS total_debit_amount,
    COUNT(DISTINCT CASE WHEN direction = 'credit'
                        THEN tx_id END)                    AS total_credit_tx,
    SUM(CASE WHEN direction = 'credit'
             THEN amount_kzt ELSE 0 END)                  AS total_credit_amount,
    COUNT(DISTINCT receiver_name)                          AS unique_receivers,
    COUNT(DISTINCT payer_name)                             AS unique_payers,
    MAX(amount_kzt)                                        AS max_transaction,
    MIN(operation_date)                                    AS data_from,
    MAX(operation_date)                                    AS data_to
FROM {_VIEW};
"""

    @staticmethod
    def high_value_to_ip(min_amount: float = 1_000_000) -> str:
        """High-value individual transactions to IP entrepreneurs."""
        return f"""
SELECT tx_id, operation_date, amount_kzt, direction,
       payer_name, receiver_name, purpose_text
FROM {_VIEW}
WHERE amount_kzt >= {min_amount}
  AND (
    LOWER(COALESCE(receiver_name, '')) LIKE '%ип %'
    OR LOWER(COALESCE(receiver_name, '')) LIKE '% ип%'
    OR LOWER(COALESCE(receiver_name, '')) LIKE '%индивидуальный%'
  )
  AND receiver_name IS NOT NULL
ORDER BY amount_kzt DESC
LIMIT 50;
"""

    @staticmethod
    def repeated_payer_receiver_pairs() -> str:
        """Entity pairs with 5+ repeated transactions — potential shell structure."""
        return f"""
SELECT
    payer_name,
    receiver_name,
    COUNT(*)              AS transaction_count,
    SUM(amount_kzt)       AS total_amount,
    AVG(amount_kzt)       AS avg_amount,
    MIN(operation_date)   AS first_tx,
    MAX(operation_date)   AS last_tx
FROM {_VIEW}
WHERE payer_name IS NOT NULL
  AND receiver_name IS NOT NULL
  AND payer_name != receiver_name
GROUP BY payer_name, receiver_name
HAVING COUNT(*) >= 5
ORDER BY transaction_count DESC, total_amount DESC
LIMIT 50;
"""

    @staticmethod
    def missing_purpose_transactions() -> str:
        """Transactions with no or very short purpose text (high risk indicator)."""
        return f"""
SELECT tx_id, operation_date, amount_kzt, direction,
       payer_name, receiver_name, purpose_text, operation_type_raw
FROM {_VIEW}
WHERE (
    purpose_text IS NULL
    OR purpose_text = ''
    OR CHAR_LENGTH(purpose_text) < 5
)
ORDER BY amount_kzt DESC
LIMIT 100;
"""

    @staticmethod
    def cash_out_obnal(min_amount: float = 100_000) -> str:
        """Debit aggregation by receiver — legacy obnal template."""
        return f"""
SELECT receiver_name,
       SUM(amount_kzt)  AS total_obnal,
       COUNT(*)         AS tx_count,
       AVG(amount_kzt)  AS avg_obnal,
       operation_type_raw
FROM {_VIEW}
WHERE direction = 'debit'
  AND amount_kzt >= {min_amount}
  AND receiver_name IS NOT NULL
GROUP BY receiver_name, operation_type_raw
ORDER BY total_obnal DESC
LIMIT 50;
"""

    @staticmethod
    def pattern_by_bank(source_bank: str) -> str:
        """Suspicious patterns (self-transfer, round amounts, missing purpose) for one bank."""
        safe = source_bank.replace("'", "''")
        return f"""
SELECT tx_id, operation_date, amount_kzt, direction,
       payer_name, receiver_name, purpose_text
FROM {_VIEW}
WHERE source_bank = '{safe}'
  AND (
    payer_name = receiver_name
    OR amount_kzt IN (1000000, 500000, 250000, 100000)
    OR purpose_text IS NULL
    OR CHAR_LENGTH(COALESCE(purpose_text,'')) < 5
  )
ORDER BY operation_date DESC
LIMIT 100;
"""


# ── Template registry (for QueryService routing) ──────────────────────────────

#: Map from entity_extractor semantic_topic → AdvancedQueryTemplates method name.
#: Only topics where the LLM consistently generates wrong SQL are routed here.
TOPIC_TEMPLATE_MAP: Dict[str, str] = {
    "transit":        "transit_accounts",
    "circular":       "circular_transactions_2hop",
    "cash_out_by_type": "obnal_by_type",
    "top_banks":      "top_banks_by_withdrawal",
}


def get_template_sql(topic: str, entities=None) -> Optional[str]:
    """
    Return pre-built SQL for a given semantic topic, or None if not routed.

    Applies simple date/year injection for topics that support it.
    """
    tpl = AdvancedQueryTemplates()

    if topic == "transit":
        return tpl.transit_accounts()

    if topic == "circular":
        return tpl.circular_transactions_2hop()

    if topic == "cash_out_by_type":
        return tpl.obnal_by_type()

    if topic == "top_banks":
        return tpl.top_banks_by_withdrawal()

    if topic == "cash_out" and entities is not None:
        # obnal_total with optional year extracted from date_range
        year = None
        if entities.date_range and entities.date_range.op == "between":
            vals = entities.date_range.value
            if vals and len(vals) == 2:
                year = vals[0].year
        return tpl.obnal_total(year=year)

    if topic == "ip_entrepreneur":
        return tpl.ip_suspicious_schemes()

    if topic == "real_estate":
        return tpl.real_estate_transactions()

    if topic == "client_incoming":
        # Generic: no specific client name — show all credit transactions
        # (LLM will handle the parameterised form when a client name is mentioned)
        return tpl.client_incoming(client_name=None)

    if topic == "period_summary":
        # Inject date range if extracted, else show all months
        from_date = to_date = None
        if entities is not None and entities.date_range and entities.date_range.op == "between":
            vals = entities.date_range.value
            if vals and len(vals) == 2:
                from_date = str(vals[0])
                to_date   = str(vals[1])
        return tpl.period_summary(from_date=from_date, to_date=to_date)

    return None


def describe_template(template_name: str) -> str:
    descriptions = {
        "circular_transactions":      "Self-transfers (payer = receiver)",
        "circular_transactions_2hop": "True circular flow A→B→A within 30 days",
        "transit_accounts":           "Pass-through accounts (in ≈ out, ≥70 % throughput)",
        "obnal_by_type":              "Cash-out grouped by operation type",
        "obnal_total":                "Total debit outflow, optionally by year",
        "ip_suspicious_schemes":      "IP entrepreneurs with high-volume suspicious receipts",
        "ip_entrepreneur_transactions": "IP transactions aggregated by receiver",
        "real_estate_transactions":   "Real estate related payments",
        "top_banks_by_withdrawal":    "Banks ranked by total debit volume",
        "client_incoming":            "All incoming transactions, optionally by client",
        "period_summary":             "Monthly credit/debit aggregation",
        "round_amount_transactions":  "Round-amount debit transactions (structuring)",
        "rapid_fire_transactions":    "Multiple transactions in short time window",
        "suspicious_patterns_summary": "One-row dashboard of suspicious indicators",
        "high_value_to_ip":           "Large individual payments to IP entrepreneurs",
        "repeated_payer_receiver_pairs": "Same entity pairs transacting repeatedly",
        "missing_purpose_transactions": "Transactions with empty/minimal purpose",
        "cash_out_obnal":             "Debit aggregation by receiver (legacy)",
        "pattern_by_bank":            "Suspicious patterns for a specific bank",
    }
    return descriptions.get(template_name, "Unknown template")


def list_templates() -> Dict[str, str]:
    tpl = AdvancedQueryTemplates()
    return {
        name: describe_template(name)
        for name in dir(tpl)
        if not name.startswith("_") and callable(getattr(tpl, name))
    }
