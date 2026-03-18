from __future__ import annotations

"""
SQL Debug Utilities

Provides tools for analyzing and debugging NL2SQL queries:
  - Pattern matching validation
  - Query structure analysis
  - Performance hints
  - Data pattern detection
"""

import re
from typing import Any, Dict, List, Optional


class SQLDebugger:
    """Analyzes SQL for common issues and patterns."""

    def __init__(self):
        pass

    def analyze(self, sql: str, rows: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Comprehensive SQL analysis.
        
        Returns dict with:
          - query_type: 'filter', 'topic', 'aggregation', 'semantic'
          - has_where: bool
          - like_patterns: List[str]
          - is_aggregation: bool
          - potential_issues: List[str]
          - performance_hints: List[str]
        """
        analysis = {
            "query_type": self._detect_query_type(sql),
            "has_where": bool(re.search(r"\bWHERE\b", sql, re.IGNORECASE)),
            "like_patterns": self._extract_like_patterns(sql),
            "is_aggregation": bool(re.search(r"\b(SUM|COUNT|AVG|MAX|MIN|GROUP BY)\b", sql, re.IGNORECASE)),
            "has_duplicates": self._check_duplicate_conditions(sql),
            "potential_issues": self._check_issues(sql),
            "performance_hints": self._suggest_optimizations(sql),
        }
        
        if rows is not None:
            analysis["result_quality"] = self._analyze_results(sql, rows)
        
        return analysis

    def _detect_query_type(self, sql: str) -> str:
        """Detect the query type (filter, topic, aggregation, semantic)."""
        sql_upper = sql.upper()
        
        if "GROUP BY" in sql_upper:
            return "aggregation"
        elif "ORDER BY semantic_embedding <->" in sql:
            return "semantic"
        elif "LIKE" in sql_upper:
            return "topic"
        else:
            return "filter"

    def _extract_like_patterns(self, sql: str) -> List[str]:
        """Extract all LIKE patterns from SQL."""
        patterns = re.findall(r"LIKE\s+'([^']+)'", sql, re.IGNORECASE)
        return patterns

    def _check_duplicate_conditions(self, sql: str) -> bool:
        """Check if there are duplicate LIKE conditions."""
        patterns = self._extract_like_patterns(sql)
        return len(patterns) != len(set(patterns))

    def _check_issues(self, sql: str) -> List[str]:
        """Check for common SQL issues."""
        issues = []
        
        # Check for SELECT *
        if re.search(r"SELECT\s+\*", sql, re.IGNORECASE):
            issues.append("SELECT * found — should specify columns")
        
        # Check for semantic_embedding selection
        if "SELECT semantic_embedding" in sql or "semantic_embedding," in sql:
            issues.append("semantic_embedding should not be SELECTed (use only in ORDER BY)")
        
        # Check for LIKE on semantic_text
        if re.search(r"semantic_text.*LIKE", sql, re.IGNORECASE) or re.search(r"LIKE.*semantic_text", sql, re.IGNORECASE):
            issues.append("LIKE on semantic_text causes false positives (has accounting terms)")
        
        # Check for GROUP BY without aggregates
        if "GROUP BY" in sql.upper() and not re.search(r"\b(SUM|COUNT|AVG|MAX|MIN)\(", sql):
            issues.append("GROUP BY without aggregation functions")
        
        # Check for missing LIMIT in non-aggregation
        if "GROUP BY" not in sql.upper() and "LIMIT" not in sql.upper():
            issues.append("Non-aggregation query missing LIMIT")
        
        # Check for duplicate LIKE patterns
        if self._check_duplicate_conditions(sql):
            issues.append("Duplicate LIKE patterns found (redundant)")
        
        return issues

    def _suggest_optimizations(self, sql: str) -> List[str]:
        """Suggest query optimizations."""
        hints = []
        
        # Suggest index usage
        if "operation_date" in sql:
            hints.append("operation_date is indexed — use for efficient filtering")
        
        # Suggest LIMIT for non-GROUP BY
        if "GROUP BY" not in sql.upper() and "LIMIT" in sql.upper():
            limit_match = re.search(r"LIMIT\s+(\d+)", sql, re.IGNORECASE)
            if limit_match:
                limit_val = int(limit_match.group(1))
                if limit_val > 500:
                    hints.append(f"LIMIT {limit_val} is large — consider LIMIT 100 for better latency")
        
        # Suggest WHERE optimization
        if "LIKE" in sql and "WHERE" not in sql:
            hints.append("LIKE without WHERE clause — may scan many rows")
        
        # Suggest NULL filtering in GROUP BY
        if "GROUP BY" in sql.upper():
            if "IS NOT NULL" not in sql:
                hints.append("GROUP BY without NULL filtering — results may include NULL group")
        
        return hints

    def _analyze_results(self, sql: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze the quality of returned results."""
        analysis = {
            "row_count": len(rows),
            "null_percentage": 0.0,
            "has_null_group": False,
        }
        
        if rows:
            # Count NULLs
            total_cells = sum(len(row) for row in rows)
            null_cells = sum(1 for row in rows for v in row.values() if v is None)
            analysis["null_percentage"] = (null_cells / total_cells * 100) if total_cells > 0 else 0
            
            # Check for NULL group (usually indicates GROUP BY issue)
            if "GROUP BY" in sql.upper():
                for row in rows:
                    group_cols = [k for k in row.keys() if k not in ("total_amount", "tx_count", "count")]
                    if any(row.get(k) is None for k in group_cols):
                        analysis["has_null_group"] = True
                        break
        
        return analysis


def print_debug_analysis(sql: str, rows: List[Dict[str, Any]] = None) -> None:
    """Pretty-print SQL debug analysis."""
    debugger = SQLDebugger()
    analysis = debugger.analyze(sql, rows)
    
    print("\n" + "=" * 70)
    print("📊 SQL DEBUG ANALYSIS")
    print("=" * 70)
    
    print(f"\nQuery Type: {analysis['query_type'].upper()}")
    print(f"Has WHERE: {analysis['has_where']}")
    print(f"Is Aggregation: {analysis['is_aggregation']}")
    
    if analysis['like_patterns']:
        print(f"\nLIKE Patterns ({len(analysis['like_patterns'])}):")
        for i, pattern in enumerate(analysis['like_patterns'], 1):
            print(f"  {i}. {pattern}")
    
    if analysis['has_duplicates']:
        print("\n⚠️  Duplicate LIKE patterns detected!")
    
    if analysis['potential_issues']:
        print("\n❌ POTENTIAL ISSUES:")
        for issue in analysis['potential_issues']:
            print(f"  • {issue}")
    
    if analysis['performance_hints']:
        print("\n💡 PERFORMANCE HINTS:")
        for hint in analysis['performance_hints']:
            print(f"  • {hint}")
    
    if 'result_quality' in analysis:
        rq = analysis['result_quality']
        print(f"\n📈 RESULT QUALITY:")
        print(f"  Rows returned: {rq['row_count']}")
        print(f"  NULL percentage: {rq['null_percentage']:.1f}%")
        if rq['has_null_group']:
            print(f"  ⚠️  NULL group detected in aggregation!")
    
    print("\n" + "=" * 70 + "\n")
