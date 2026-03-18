"""
Fraud Detection Patterns and Scoring

Provides analytical models for detecting financial crimes:
- Anomaly detection (statistical deviations)
- Behavioral analysis (pattern changes)
- Network analysis (graph-based detection)
- Scheme detection (known fraud patterns)
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
import statistics


@dataclass
class TransactionAnomaly:
    """Represents a detected transaction anomaly."""
    tx_id: str
    anomaly_type: str  # "amount", "frequency", "counterparty", "timing"
    score: float  # 0-100
    reason: str
    severity: str  # "LOW", "MEDIUM", "HIGH", "CRITICAL"


@dataclass
class EntityRiskProfile:
    """Risk assessment for an entity (payer or receiver)."""
    entity_name: str
    risk_score: float  # 0-100
    total_transactions: int
    total_amount: float
    avg_amount: float
    suspicious_indicators: List[str]
    last_activity: datetime


class AnomalyDetector:
    """Statistical anomaly detection for transactions."""

    @staticmethod
    def detect_amount_anomaly(
        transaction_amount: float,
        historical_amounts: List[float],
        threshold_std: float = 2.0
    ) -> Tuple[bool, float]:
        """
        Detect if transaction amount is anomalous.
        
        Args:
            transaction_amount: Current transaction amount
            historical_amounts: Past transaction amounts for same entity
            threshold_std: Standard deviation multiplier (2.0 = ~95% confidence)
        
        Returns:
            (is_anomalous, z_score)
        """
        if len(historical_amounts) < 3:
            return False, 0.0
        
        mean = statistics.mean(historical_amounts)
        try:
            stdev = statistics.stdev(historical_amounts)
            if stdev == 0:
                return False, 0.0
            
            z_score = abs((transaction_amount - mean) / stdev)
            is_anomalous = z_score > threshold_std
            return is_anomalous, min(z_score, 10.0)  # Cap at 10.0
        except (statistics.StatisticsError, ZeroDivisionError):
            return False, 0.0

    @staticmethod
    def detect_frequency_anomaly(
        tx_timestamps: List[datetime],
        window_minutes: int = 60
    ) -> Tuple[int, float]:
        """
        Detect rapid-fire transaction patterns.
        
        Args:
            tx_timestamps: Timestamps of recent transactions
            window_minutes: Time window to check
        
        Returns:
            (transactions_in_window, anomaly_score)
        """
        if len(tx_timestamps) < 2:
            return 0, 0.0
        
        # Sort timestamps
        sorted_ts = sorted(tx_timestamps)
        now = datetime.now()
        recent_ts = [ts for ts in sorted_ts if (now - ts).total_seconds() < window_minutes * 60]
        
        anomaly_score = 0.0
        if len(recent_ts) >= 3:
            anomaly_score = min(len(recent_ts) * 15.0, 100.0)  # 3+ = 45+, cap at 100
        
        return len(recent_ts), anomaly_score

    @staticmethod
    def detect_round_amount_pattern(amount: float) -> Tuple[bool, str]:
        """Detect round amounts commonly used in obnal schemes."""
        round_amounts = {
            1000000: "1M",
            500000: "500K",
            250000: "250K",
            100000: "100K",
            50000: "50K",
            25000: "25K",
            10000: "10K",
        }
        
        if amount in round_amounts:
            return True, round_amounts[amount]
        
        # Check for multiples of 25000 (common in obnal)
        if amount > 10000 and amount % 25000 == 0:
            return True, f"{amount}K"
        
        return False, ""

    @staticmethod
    def detect_counterparty_anomaly(
        payer: str,
        receiver: str,
        historical_counterparties: List[str]
    ) -> Tuple[bool, float]:
        """
        Detect unusual counterparty relationships.
        
        Args:
            payer: Payer entity name
            receiver: Receiver entity name
            historical_counterparties: List of historically normal receivers
        
        Returns:
            (is_anomalous, anomaly_score)
        """
        # Check if self-transfer
        if payer.lower().strip() == receiver.lower().strip():
            return True, 90.0
        
        # Check if never interacted before
        if receiver not in historical_counterparties and len(historical_counterparties) > 5:
            return True, 60.0
        
        return False, 0.0


class BehavioralAnalyzer:
    """Behavioral pattern analysis for entities."""

    @staticmethod
    def analyze_entity_behavior(
        entity_name: str,
        transactions: List[Dict[str, Any]]
    ) -> EntityRiskProfile:
        """
        Analyze behavioral patterns for an entity.
        
        Args:
            entity_name: Entity name (payer or receiver)
            transactions: List of transactions involving entity
        
        Returns:
            EntityRiskProfile with risk assessment
        """
        if not transactions:
            return EntityRiskProfile(
                entity_name=entity_name,
                risk_score=0.0,
                total_transactions=0,
                total_amount=0.0,
                avg_amount=0.0,
                suspicious_indicators=[],
                last_activity=datetime.now()
            )

        amounts = [tx["amount_kzt"] for tx in transactions]
        total_amount = sum(amounts)
        avg_amount = total_amount / len(transactions)

        suspicious_indicators = []
        risk_components = []

        # Check 1: Very high average transaction amount
        if avg_amount > 1000000:
            suspicious_indicators.append("High average transaction amount")
            risk_components.append(40)

        # Check 2: All transactions are round amounts
        round_amounts_count = sum(1 for tx in transactions if tx["amount_kzt"] % 25000 == 0)
        if round_amounts_count / len(transactions) > 0.7:
            suspicious_indicators.append("Predominantly round amounts")
            risk_components.append(35)

        # Check 3: High frequency
        if len(transactions) > 20:
            suspicious_indicators.append("High transaction frequency")
            risk_components.append(25)

        # Check 4: Consistent with single purpose
        purposes = set(tx.get("purpose_text", "").strip() for tx in transactions)
        if len(purposes) <= 2:
            suspicious_indicators.append("Repetitive transaction patterns")
            risk_components.append(20)

        # Check 5: Missing purpose information
        missing_purpose = sum(1 for tx in transactions 
                            if not tx.get("purpose_text") or len(str(tx.get("purpose_text", ""))) < 3)
        if missing_purpose / len(transactions) > 0.3:
            suspicious_indicators.append("High % of missing purpose")
            risk_components.append(30)

        # Calculate overall risk
        if risk_components:
            risk_score = min(sum(risk_components) / len(risk_components), 100.0)
        else:
            risk_score = 0.0

        # Get last activity
        last_activity = max(tx.get("operation_ts", datetime.now()) for tx in transactions)

        return EntityRiskProfile(
            entity_name=entity_name,
            risk_score=risk_score,
            total_transactions=len(transactions),
            total_amount=total_amount,
            avg_amount=avg_amount,
            suspicious_indicators=suspicious_indicators,
            last_activity=last_activity
        )

    @staticmethod
    def detect_behavior_change(
        historical_profile: EntityRiskProfile,
        current_profile: EntityRiskProfile
    ) -> Tuple[bool, float]:
        """
        Detect sudden behavior changes.
        
        Returns:
            (behavior_changed, change_score)
        """
        if not historical_profile or historical_profile.total_transactions == 0:
            return False, 0.0

        # Check for significant increase in avg amount
        amount_ratio = current_profile.avg_amount / (historical_profile.avg_amount or 1)
        if amount_ratio > 2.0:
            return True, min((amount_ratio - 1) * 30, 100.0)

        # Check for frequency increase
        if current_profile.total_transactions > historical_profile.total_transactions * 1.5:
            return True, 50.0

        return False, 0.0


class SchemeDetector:
    """Detection of known fraud schemes."""

    @staticmethod
    def detect_circular_scheme(payer: str, receiver: str) -> bool:
        """Detect self-transfer circular scheme."""
        return payer.lower().strip() == receiver.lower().strip()

    @staticmethod
    def detect_layering_pattern(
        transactions: List[Dict[str, Any]],
        max_depth: int = 5
    ) -> Tuple[bool, List[str]]:
        """
        Detect money laundering layering (A→B→C→D chain).
        
        Returns:
            (is_layering_detected, chain_descriptions)
        """
        # Build transaction graph
        graph: Dict[str, List[str]] = {}
        for tx in transactions:
            payer = tx.get("payer_name", "")
            receiver = tx.get("receiver_name", "")
            if payer and receiver:
                if payer not in graph:
                    graph[payer] = []
                graph[payer].append(receiver)

        # Find chains
        chains = []

        def find_chains(start: str, current_path: List[str], depth: int):
            if depth >= max_depth:
                return

            if start in graph:
                for next_entity in graph[start]:
                    new_path = current_path + [next_entity]
                    if len(new_path) >= 3:
                        chains.append(" → ".join(new_path))
                    find_chains(next_entity, new_path, depth + 1)

        # Find all chains
        for entity in graph:
            find_chains(entity, [entity], 0)

        return len(chains) > 0, chains[:5]  # Return top 5 chains

    @staticmethod
    def detect_obnal_scheme(
        transactions: List[Dict[str, Any]],
        min_total_amount: float = 500000
    ) -> Tuple[bool, float]:
        """
        Detect obnal (cash out) scheme.
        
        Indicators:
        - Debit transactions to individuals/merchants
        - Round amounts
        - High frequency
        - Missing purpose
        """
        if not transactions:
            return False, 0.0

        debit_txs = [tx for tx in transactions if tx.get("direction") == "debit"]
        if not debit_txs:
            return False, 0.0

        # Check indicators
        score = 0.0

        # Indicator 1: Transactions are debits
        debit_ratio = len(debit_txs) / len(transactions)
        if debit_ratio > 0.7:
            score += 20

        # Indicator 2: Round amounts
        round_count = sum(1 for tx in debit_txs if tx["amount_kzt"] % 25000 == 0)
        if round_count / len(debit_txs) > 0.5:
            score += 25

        # Indicator 3: Missing purpose
        missing_purpose = sum(1 for tx in debit_txs 
                            if not tx.get("purpose_text") or len(str(tx.get("purpose_text"))) < 3)
        if missing_purpose / len(debit_txs) > 0.3:
            score += 20

        # Indicator 4: High frequency
        if len(debit_txs) > 15:
            score += 15

        # Indicator 5: Total amount threshold
        total_amount = sum(tx["amount_kzt"] for tx in debit_txs)
        if total_amount >= min_total_amount:
            score += 10

        return score >= 60.0, min(score, 100.0)

    @staticmethod
    def detect_real_estate_anomaly(transactions: List[Dict[str, Any]]) -> Tuple[bool, float]:
        """
        Detect unusual real estate transaction patterns.
        """
        if not transactions:
            return False, 0.0

        re_txs = [tx for tx in transactions 
                 if "недвижим" in (tx.get("purpose_text") or "").lower()
                 or "квартир" in (tx.get("purpose_text") or "").lower()]

        if not re_txs:
            return False, 0.0

        score = 0.0

        # High-value real estate transactions
        high_value_count = sum(1 for tx in re_txs if tx["amount_kzt"] > 5000000)
        if high_value_count > 0:
            score += 20 * min(high_value_count, 3)

        # Frequency indicator
        if len(re_txs) > 5:
            score += 30

        # Missing documentation
        missing_purpose = sum(1 for tx in re_txs if not tx.get("purpose_text"))
        if missing_purpose / len(re_txs) > 0.5:
            score += 25

        return score >= 50.0, min(score, 100.0)


class RiskScorer:
    """Combined risk scoring."""

    @staticmethod
    def calculate_transaction_risk(
        transaction: Dict[str, Any],
        historical_data: Dict[str, Any] = None
    ) -> float:
        """
        Calculate overall risk score (0-100) for a transaction.
        
        Args:
            transaction: Transaction dict
            historical_data: Optional historical context
        
        Returns:
            Risk score 0-100
        """
        score = 0.0

        # Amount anomaly
        if historical_data and "historical_amounts" in historical_data:
            is_anomalous, z_score = AnomalyDetector.detect_amount_anomaly(
                transaction["amount_kzt"],
                historical_data["historical_amounts"]
            )
            if is_anomalous:
                score += min(z_score * 10, 40)

        # Round amount
        is_round, _ = AnomalyDetector.detect_round_amount_pattern(transaction["amount_kzt"])
        if is_round:
            score += 15

        # Self-transfer
        if transaction.get("payer_name") == transaction.get("receiver_name"):
            score += 40

        # Missing purpose
        if not transaction.get("purpose_text") or len(str(transaction.get("purpose_text"))) < 3:
            score += 20

        # Debit direction
        if transaction.get("direction") == "debit":
            score += 10

        return min(score, 100.0)

    @staticmethod
    def calculate_entity_risk(profile: EntityRiskProfile) -> float:
        """Calculate overall risk score for an entity."""
        return profile.risk_score


if __name__ == "__main__":
    print("🔍 Fraud Detection Patterns Module")
    print("=" * 50)
    print()
    print("Available Classes:")
    print("  - AnomalyDetector: Statistical anomaly detection")
    print("  - BehavioralAnalyzer: Behavioral pattern analysis")
    print("  - SchemeDetector: Known fraud scheme detection")
    print("  - RiskScorer: Combined risk calculation")
    print()
    print("Key Methods:")
    print("  - detect_amount_anomaly: Z-score based detection")
    print("  - detect_frequency_anomaly: Rapid-fire detection")
    print("  - detect_circular_scheme: Self-transfer detection")
    print("  - detect_obnal_scheme: Cash-out scheme detection")
    print("  - calculate_transaction_risk: Overall risk scoring")
