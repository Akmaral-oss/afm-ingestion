#!/usr/bin/env python3
"""
Enhanced Templates and Fraud Detection CLI

Commands:
  templates list              - List all available query templates
  templates describe <name>   - Show template details
  templates sql <name>        - Print SQL for template
  templates run <name>        - Execute template query
  
  fraud analyze-tx <tx_id>    - Analyze single transaction for fraud risk
  fraud analyze-entity <name> - Analyze entity risk profile
  fraud patterns <type>       - Find specific fraud patterns
  
  test-data generate          - Generate test fraud data
  test-data insert            - Insert test data into database
"""

import argparse
import sys
from typing import Optional, Dict, Any
import json
from datetime import datetime

# Project imports
from app.db.engine import get_session
from app.db.schema import Transaction
from app.nl2sql.advanced_templates import AdvancedQueryTemplates, list_templates
from app.nl2sql.fraud_patterns import (
    AnomalyDetector, BehavioralAnalyzer, SchemeDetector, RiskScorer,
    TransactionAnomaly, EntityRiskProfile
)
from app.nl2sql.query_service import QueryService
from scripts.generate_test_data import TransactionGenerator


def print_header(title: str):
    """Print formatted section header."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def print_risk_indicator(score: float) -> str:
    """Return emoji indicator for risk score."""
    if score >= 80:
        return "🔴 CRITICAL"
    elif score >= 60:
        return "🟠 HIGH"
    elif score >= 40:
        return "🟡 MEDIUM"
    elif score >= 20:
        return "🟢 LOW"
    else:
        return "⚪ MINIMAL"


# ============================================================================
# TEMPLATES COMMANDS
# ============================================================================

def cmd_templates_list():
    """List all available templates."""
    print_header("Available Query Templates")
    
    templates = list_templates()
    
    for i, (name, description) in enumerate(templates.items(), 1):
        print(f"{i:2d}. {name}")
        print(f"    {description}\n")
    
    print(f"Total: {len(templates)} templates")
    print("\nUse 'templates describe <name>' for details")
    print("Use 'templates sql <name>' to view SQL")
    print("Use 'templates run <name>' to execute")


def cmd_templates_describe(name: str):
    """Show template details."""
    templates = AdvancedQueryTemplates()
    
    if not hasattr(templates, name):
        print(f"❌ Template '{name}' not found")
        return
    
    print_header(f"Template: {name}")
    
    # Get the method
    method = getattr(templates, name)
    if callable(method):
        # Try to get docstring
        if method.__doc__:
            print(method.__doc__)
        else:
            print(f"(No description available)")
    
    print(f"\nSQL Preview:")
    print("-" * 60)
    
    try:
        if name == "cash_out_obnal" or name == "rapid_fire_transactions" or name == "pattern_by_bank":
            # These have required parameters
            print("(This template requires parameters)")
            print(f"\nExample: templates run {name}")
        else:
            sql = method()
            print(sql)
    except Exception as e:
        print(f"Error: {e}")


def cmd_templates_sql(name: str):
    """Print SQL for template."""
    templates = AdvancedQueryTemplates()
    
    if not hasattr(templates, name):
        print(f"❌ Template '{name}' not found")
        return
    
    method = getattr(templates, name)
    
    try:
        if name == "cash_out_obnal":
            sql = method(min_amount=100000)
        elif name == "rapid_fire_transactions":
            sql = method(time_window_hours=1)
        elif name == "pattern_by_bank":
            sql = method(source_bank="HALYK")
        else:
            sql = method()
        
        print(sql)
    except Exception as e:
        print(f"❌ Error: {e}")


def cmd_templates_run(name: str):
    """Execute template query."""
    print_header(f"Executing: {name}")
    
    service = QueryService()
    templates = AdvancedQueryTemplates()
    
    if not hasattr(templates, name):
        print(f"❌ Template '{name}' not found")
        return
    
    method = getattr(templates, name)
    
    try:
        if name == "cash_out_obnal":
            sql = method(min_amount=100000)
        elif name == "rapid_fire_transactions":
            sql = method(time_window_hours=1)
        elif name == "pattern_by_bank":
            sql = method(source_bank="HALYK")
        else:
            sql = method()
        
        print(f"SQL:\n{sql}\n")
        
        # Execute
        try:
            result = service.run_raw_sql(sql)
            print(f"✅ Results: {len(result)} rows\n")
            
            if result:
                # Print first few rows
                keys = result[0].keys() if result else []
                print("Sample rows:")
                for row in result[:5]:
                    for key in keys:
                        print(f"  {key}: {row[key]}")
                    print()
        except Exception as e:
            print(f"⚠️  Execution error: {e}")
    
    except Exception as e:
        print(f"❌ Error: {e}")


# ============================================================================
# FRAUD DETECTION COMMANDS
# ============================================================================

def cmd_fraud_analyze_tx(tx_id: str):
    """Analyze single transaction for fraud risk."""
    print_header(f"Transaction Risk Analysis: {tx_id}")
    
    session = get_session()
    tx = session.query(Transaction).filter(Transaction.tx_id == tx_id).first()
    
    if not tx:
        print(f"❌ Transaction '{tx_id}' not found")
        return
    
    tx_dict = {
        "tx_id": tx.tx_id,
        "amount_kzt": tx.amount_kzt,
        "direction": tx.direction,
        "payer_name": tx.payer_name,
        "receiver_name": tx.receiver_name,
        "purpose_text": tx.purpose_text,
        "operation_date": tx.operation_date,
        "operation_type_raw": tx.operation_type_raw,
    }
    
    # Basic info
    print("Transaction Details:")
    print(f"  Date: {tx.operation_date}")
    print(f"  Amount: {tx.amount_kzt:,.2f} KZT")
    print(f"  Direction: {tx.direction}")
    print(f"  From: {tx.payer_name}")
    print(f"  To: {tx.receiver_name}")
    print(f"  Purpose: {tx.purpose_text or '(missing)'}")
    print()
    
    # Risk Analysis
    print("Risk Indicators:")
    
    # Check 1: Round amount
    is_round, round_name = AnomalyDetector.detect_round_amount_pattern(tx.amount_kzt)
    print(f"  Round Amount: {'✅ YES (' + round_name + ')' if is_round else '❌ NO'}")
    
    # Check 2: Self-transfer
    is_circular = SchemeDetector.detect_circular_scheme(tx.payer_name or "", tx.receiver_name or "")
    print(f"  Self-Transfer: {'✅ YES' if is_circular else '❌ NO'}")
    
    # Check 3: Missing purpose
    missing = not tx.purpose_text or len(str(tx.purpose_text)) < 3
    print(f"  Missing Purpose: {'✅ YES' if missing else '❌ NO'}")
    
    # Check 4: Debit
    is_debit = tx.direction == "debit"
    print(f"  Debit Direction: {'✅ YES' if is_debit else '❌ NO'}")
    
    # Overall risk
    print()
    risk_score = RiskScorer.calculate_transaction_risk(tx_dict)
    print(f"Overall Risk: {print_risk_indicator(risk_score)}")
    print(f"Risk Score: {risk_score:.1f}/100")


def cmd_fraud_analyze_entity(entity_name: str):
    """Analyze entity risk profile."""
    print_header(f"Entity Risk Profile: {entity_name}")
    
    session = get_session()
    
    # Get transactions for this entity
    txs = session.query(Transaction).filter(
        (Transaction.payer_name == entity_name) | 
        (Transaction.receiver_name == entity_name)
    ).all()
    
    if not txs:
        print(f"❌ No transactions found for entity '{entity_name}'")
        return
    
    # Convert to dict format
    tx_dicts = [{
        "amount_kzt": tx.amount_kzt,
        "direction": tx.direction,
        "payer_name": tx.payer_name,
        "receiver_name": tx.receiver_name,
        "purpose_text": tx.purpose_text,
        "operation_ts": tx.operation_ts,
        "operation_type_raw": tx.operation_type_raw,
    } for tx in txs]
    
    # Analyze behavior
    profile = BehavioralAnalyzer.analyze_entity_behavior(entity_name, tx_dicts)
    
    print("Entity Profile:")
    print(f"  Name: {profile.entity_name}")
    print(f"  Total Transactions: {profile.total_transactions}")
    print(f"  Total Amount: {profile.total_amount:,.2f} KZT")
    print(f"  Average Amount: {profile.avg_amount:,.2f} KZT")
    print(f"  Last Activity: {profile.last_activity.strftime('%Y-%m-%d %H:%M')}")
    print()
    
    print("Suspicious Indicators:")
    if profile.suspicious_indicators:
        for indicator in profile.suspicious_indicators:
            print(f"  ⚠️  {indicator}")
    else:
        print("  (None detected)")
    print()
    
    print(f"Risk Profile: {print_risk_indicator(profile.risk_score)}")
    print(f"Risk Score: {profile.risk_score:.1f}/100")


def cmd_fraud_patterns(pattern_type: str):
    """Find specific fraud patterns."""
    print_header(f"Fraud Pattern Detection: {pattern_type}")
    
    session = get_session()
    
    if pattern_type == "circular":
        print("🔍 Looking for circular transactions (self-transfers)...")
        txs = session.query(Transaction).filter(
            Transaction.payer_name == Transaction.receiver_name
        ).limit(20).all()
        
        if txs:
            print(f"Found {len(txs)} circular transactions:\n")
            for tx in txs:
                print(f"  {tx.operation_date} | {tx.amount_kzt:>10,.0f} KZT | {tx.payer_name}")
        else:
            print("No circular transactions found")
    
    elif pattern_type == "round_amounts":
        print("🔍 Looking for round amount transactions...")
        txs = session.query(Transaction).filter(
            (Transaction.amount_kzt.in_([1000000, 500000, 250000, 100000, 50000, 25000]))
        ).limit(20).all()
        
        if txs:
            print(f"Found {len(txs)} round amount transactions:\n")
            for tx in txs:
                is_round, name = AnomalyDetector.detect_round_amount_pattern(tx.amount_kzt)
                print(f"  {tx.operation_date} | {name:>5s} | {tx.payer_name} → {tx.receiver_name}")
        else:
            print("No round amount transactions found")
    
    elif pattern_type == "missing_purpose":
        print("🔍 Looking for missing purpose transactions...")
        txs = session.query(Transaction).filter(
            (Transaction.purpose_text.is_(None)) | 
            (Transaction.purpose_text == '')
        ).limit(20).all()
        
        if txs:
            print(f"Found {len(txs)} transactions with missing purpose:\n")
            for tx in txs:
                print(f"  {tx.operation_date} | {tx.amount_kzt:>10,.0f} | {tx.payer_name} → {tx.receiver_name}")
        else:
            print("No missing purpose transactions found")
    
    elif pattern_type == "debit_heavy":
        print("🔍 Looking for debit-heavy entities...")
        from sqlalchemy import func
        
        debit_counts = session.query(
            Transaction.payer_name,
            func.count().label('debit_count'),
            func.sum(Transaction.amount_kzt).label('total_amount')
        ).filter(Transaction.direction == 'debit').group_by(
            Transaction.payer_name
        ).order_by(func.count().desc()).limit(10).all()
        
        if debit_counts:
            print(f"Top 10 debit entities:\n")
            for name, count, total in debit_counts:
                print(f"  {name:30s} | {count:3d} txs | {total:>12,.0f} KZT")
        else:
            print("No debit transactions found")
    
    else:
        print(f"❌ Unknown pattern type: {pattern_type}")
        print("Available patterns: circular, round_amounts, missing_purpose, debit_heavy")


# ============================================================================
# TEST DATA COMMANDS
# ============================================================================

def cmd_test_data_generate(count: int = 1):
    """Generate test fraud data."""
    print_header("Generating Test Fraud Data")
    
    print(f"Generating sample transactions...")
    
    data = TransactionGenerator.generate_all()
    
    print(f"✅ Generated {len(data)} test transactions")
    print()
    print("Summary:")
    print(f"  - Circular transactions: 20")
    print(f"  - Real estate: 50")
    print(f"  - IP entrepreneurs: 30")
    print(f"  - Cash out: 100")
    print(f"  - Rapid fire: 50")
    print(f"  - Transit accounts: 40")
    print(f"  - Missing purpose: 30")
    print()
    print("To insert into database, use: test-data insert")


def cmd_test_data_insert():
    """Insert test data into database."""
    print_header("Inserting Test Data into Database")
    
    session = get_session()
    
    print("Generating data...")
    data = TransactionGenerator.generate_all()
    
    print(f"Inserting {len(data)} transactions...")
    
    from app.db.schema import Transaction
    
    count = 0
    for tx_dict in data:
        # Check if exists
        existing = session.query(Transaction).filter(
            Transaction.tx_id == tx_dict["tx_id"]
        ).first()
        
        if not existing:
            tx = Transaction(**tx_dict)
            session.add(tx)
            count += 1
    
    session.commit()
    print(f"✅ Inserted {count} new transactions")
    print(f"   Total transactions in database: {session.query(Transaction).count()}")


# ============================================================================
# MAIN CLI
# ============================================================================

def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Advanced Templates and Fraud Detection CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/advanced_cli.py templates list
  python scripts/advanced_cli.py templates describe circular_transactions
  python scripts/advanced_cli.py templates run circular_transactions
  python scripts/advanced_cli.py fraud analyze-tx <id>
  python scripts/advanced_cli.py fraud analyze-entity <name>
  python scripts/advanced_cli.py fraud patterns circular
  python scripts/advanced_cli.py test-data generate
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command category")
    
    # Templates commands
    templates_parser = subparsers.add_parser("templates", help="Query templates")
    templates_subparsers = templates_parser.add_subparsers(dest="templates_cmd")
    templates_subparsers.add_parser("list", help="List all templates")
    
    desc_parser = templates_subparsers.add_parser("describe", help="Describe template")
    desc_parser.add_argument("name", help="Template name")
    
    sql_parser = templates_subparsers.add_parser("sql", help="Print template SQL")
    sql_parser.add_argument("name", help="Template name")
    
    run_parser = templates_subparsers.add_parser("run", help="Execute template")
    run_parser.add_argument("name", help="Template name")
    
    # Fraud commands
    fraud_parser = subparsers.add_parser("fraud", help="Fraud detection")
    fraud_subparsers = fraud_parser.add_subparsers(dest="fraud_cmd")
    
    tx_parser = fraud_subparsers.add_parser("analyze-tx", help="Analyze transaction")
    tx_parser.add_argument("tx_id", help="Transaction ID")
    
    entity_parser = fraud_subparsers.add_parser("analyze-entity", help="Analyze entity")
    entity_parser.add_argument("name", help="Entity name")
    
    pattern_parser = fraud_subparsers.add_parser("patterns", help="Find patterns")
    pattern_parser.add_argument("type", help="Pattern type (circular, round_amounts, etc)")
    
    # Test data commands
    test_parser = subparsers.add_parser("test-data", help="Test data management")
    test_subparsers = test_parser.add_subparsers(dest="test_cmd")
    test_subparsers.add_parser("generate", help="Generate test data")
    test_subparsers.add_parser("insert", help="Insert test data")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        if args.command == "templates":
            if args.templates_cmd == "list":
                cmd_templates_list()
            elif args.templates_cmd == "describe":
                cmd_templates_describe(args.name)
            elif args.templates_cmd == "sql":
                cmd_templates_sql(args.name)
            elif args.templates_cmd == "run":
                cmd_templates_run(args.name)
            else:
                templates_parser.print_help()
        
        elif args.command == "fraud":
            if args.fraud_cmd == "analyze-tx":
                cmd_fraud_analyze_tx(args.tx_id)
            elif args.fraud_cmd == "analyze-entity":
                cmd_fraud_analyze_entity(args.name)
            elif args.fraud_cmd == "patterns":
                cmd_fraud_patterns(args.type)
            else:
                fraud_parser.print_help()
        
        elif args.command == "test-data":
            if args.test_cmd == "generate":
                cmd_test_data_generate()
            elif args.test_cmd == "insert":
                cmd_test_data_insert()
            else:
                test_parser.print_help()
        
        return 0
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
