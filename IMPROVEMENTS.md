# NL2SQL Query System — Improvements & Fixes

## Overview

This document summarizes all improvements made to the NL2SQL query system to enhance query quality, eliminate redundancy, and provide better debugging capabilities.

---

## ✅ Improvements Applied

### 1. **SQL Deduplication** (`app/nl2sql/sql_generator.py`)

**Problem:** Generated SQL sometimes contains duplicate LIKE conditions:
```sql
WHERE (
    LIKE '%налог%'  -- duplicate
    OR LIKE '%кпн%'
    OR LIKE '%налог%'  -- duplicate again
)
```

**Solution:** Added `_deduplicate_where_conditions()` method that:
- Extracts all LIKE patterns from generated SQL
- Tracks patterns in a set to identify duplicates
- Removes duplicate LIKE conditions
- Preserves order and validity

**Result:**
```sql
WHERE (
    LIKE '%налог%'  -- appears once
    OR LIKE '%кпн%'
)
```

---

### 2. **Enhanced Semantic Keywords** (`app/nl2sql/entity_extractor.py`)

**Problem:** Query "штрафы и пени" (penalties) returned no results even though the semantic topic was recognized.

**Solution:** Enhanced pattern matching:
```python
# Before:
r"штраф|пеня|penalty|fine": "penalty"

# After:
r"штраф|штрафн|пеня|penalty|fine": "penalty"
```

Added `штрафн` to catch variations like "штрафные" (penalty-related terms).

---

### 3. **NULL Handling in Aggregations** (`app/nl2sql/prompt_builder.py`)

**Problem:** GROUP BY queries returned NULL entries polluting results:
```
receiver_name: null
total_amount: 1638863.97  ← NULL group
```

**Solution:** Updated TYPE 5 (aggregation) template:
```sql
-- Before:
SELECT receiver_name, SUM(amount_kzt) AS total_amount
FROM afm.transactions_nl_view
WHERE <filters>
GROUP BY receiver_name

-- After:
SELECT receiver_name, SUM(amount_kzt) AS total_amount
FROM afm.transactions_nl_view
WHERE receiver_name IS NOT NULL  ← Filter NULLs
  AND <filters>
GROUP BY receiver_name
```

Now added explicit rules in prompt:
- Filter `WHERE receiver_name IS NOT NULL` for receiver aggregations
- Filter `WHERE payer_name IS NOT NULL` for payer aggregations
- Filter `WHERE operation_date IS NOT NULL` for date aggregations

---

### 4. **Query Result Quality Warnings** (`app/nl2sql/query_service.py`)

**Problem:** Users don't know why a query returned 0 rows or has NULL values.

**Solution:** Added `QueryResult.quality_warnings` field with intelligent checks:

```python
@dataclass
class QueryResult:
    question: str
    sql: str
    rows: List[Dict[str, Any]]
    execution_time_s: float
    repaired: bool = False
    error: Optional[str] = None
    quality_warnings: List[str] = field(default_factory=list)
```

Checks include:
- **No results with filters:** "No results found. Try using broader search term"
- **GROUP BY without aggregates:** Warns about duplicate groups
- **High NULL percentage (>20%):** "Results contain many NULLs. Consider filtering..."

---

### 5. **Enhanced CLI Output** (`scripts/query_cli.py`)

**Problem:** Warnings and quality issues not displayed to users.

**Solution:** Updated output to show:
```
SQL:
<query>

Rows returned : 100
Execution time: 0.031s

⚠️  WARNINGS:
  • No results found with the applied filters...
  • Results contain 45/100 rows with NULL values...

[result data]
```

---

### 6. **SQL Repair with Contextual Suggestions** (`app/nl2sql/sql_repair.py`)

**Problem:** When SQL fails, only error message is shown without guidance.

**Solution:** Enhanced repair prompt with contextual suggestions:

```python
def _suggest_fixes(self, sql: str, error: str) -> str:
    """Generate contextual fix suggestions based on error type."""
    # Analyzes error and provides specific guidance:
    # - Missing LIMIT
    # - Duplicate LIKE conditions
    # - Invalid column names
    # - Syntax errors
    # - semantic_embedding issues
```

---

### 7. **SQL Debug Utility** (`app/nl2sql/sql_debug.py`)

**New Feature:** Comprehensive SQL analysis tool:

```python
class SQLDebugger:
    def analyze(self, sql: str, rows=None) -> Dict[str, Any]:
        """Returns analysis with:
        - query_type: filter|topic|aggregation|semantic
        - like_patterns: List of LIKE anchors
        - has_duplicates: bool
        - potential_issues: List[str]
        - performance_hints: List[str]
        - result_quality: null percentage, NULL groups
        """
```

---

### 8. **Debug CLI Script** (`scripts/debug_nl2sql.py`)

**New Feature:** Standalone debug tool for analyzing queries:

```bash
# Analyze raw SQL
python scripts/debug_nl2sql.py --sql "SELECT ... FROM afm.transactions_nl_view"

# Analyze question end-to-end
python scripts/debug_nl2sql.py \
    --question "топ 10 получателей по сумме" \
    --pg "postgresql://..." \
    --model BAAI/bge-m3 \
    --llm_url http://localhost:11434 \
    --llm_model qwen2.5-coder:7b
```

Output includes:
```
📊 SQL DEBUG ANALYSIS
==================================
Query Type: AGGREGATION
Has WHERE: True
Is Aggregation: True

LIKE Patterns (2):
  1. %налог%
  2. %кпн%

❌ POTENTIAL ISSUES:
  • GROUP BY without NULL filtering

💡 PERFORMANCE HINTS:
  • operation_date is indexed
  • GROUP BY without NULL filtering
```

---

## 🚀 Quick Testing Guide

### Test 1: Deduplication
```bash
python scripts/query_cli.py \
  --pg "postgresql+psycopg2://afm_user:123%21@localhost:5433/afm_db" \
  --model BAAI/bge-m3 \
  --llm_url http://localhost:11434 \
  --llm_model qwen2.5-coder:7b \
  "штрафы и пени"
```

Expected: No duplicate LIKE conditions in generated SQL

### Test 2: NULL Filtering
```bash
python scripts/query_cli.py \
  --pg "postgresql+psycopg2://afm_user:123%21@localhost:5433/afm_db" \
  --model BAAI/bge-m3 \
  --llm_url http://localhost:11434 \
  --llm_model qwen2.5-coder:7b \
  "топ 10 получателей по сумме"
```

Expected: 
- `WHERE receiver_name IS NOT NULL` in SQL
- No NULL receiver_name in results

### Test 3: Quality Warnings
```bash
python scripts/query_cli.py \
  --pg "postgresql+psycopg2://afm_user:123%21@localhost:5433/afm_db" \
  --model BAAI/bge-m3 \
  --llm_url http://localhost:11434 \
  --llm_model qwen2.5-coder:7b \
  "несуществующая категория"
```

Expected:
- Results in 0 rows (if data doesn't exist)
- Warning: "No results found with the applied filters..."

### Test 4: Debug Analyzer
```bash
python scripts/debug_nl2sql.py \
  --sql "SELECT tx_id FROM afm.transactions_nl_view WHERE (LIKE '%x%' OR LIKE '%x%')"
```

Expected: Detects duplicate LIKE patterns

---

## 📋 Files Modified

1. **app/nl2sql/sql_generator.py**
   - Added `_deduplicate_where_conditions()` method
   - Added `List` import

2. **app/nl2sql/entity_extractor.py**
   - Enhanced `_SEMANTIC_KEYWORDS` with `штрафн` pattern

3. **app/nl2sql/prompt_builder.py**
   - Updated TYPE 5 aggregation template with NULL filtering rules
   - Added detailed LIKE deduplication hints
   - Added aggregation-specific NULL rules

4. **app/nl2sql/query_service.py**
   - Added `quality_warnings` field to `QueryResult`
   - Added `_check_result_quality()` method
   - Added `field` import from dataclasses

5. **app/nl2sql/sql_repair.py**
   - Added contextual error suggestions
   - Enhanced repair prompt with intelligent hints

6. **scripts/query_cli.py**
   - Added quality warnings output with emoji indicators
   - Improved result display formatting

## 📁 New Files Created

1. **app/nl2sql/sql_debug.py**
   - `SQLDebugger` class for comprehensive SQL analysis
   - `print_debug_analysis()` function for pretty output

2. **scripts/debug_nl2sql.py**
   - Standalone CLI tool for SQL debugging
   - Two modes: raw SQL analysis or full pipeline analysis

---

## 🎯 Key Benefits

| Issue | Before | After |
|-------|--------|-------|
| Duplicate LIKE conditions | Occurred frequently | Automatically removed |
| NULL groups in results | Visible and confusing | Filtered out automatically |
| Penalty queries | "штраф" only, missed variations | Catches "штраф", "штрафн", "пеня" |
| Zero-result feedback | Silent failure | Helpful warning message |
| Debug capability | Manual SQL inspection | Automated analysis tool |
| Error recovery | Generic repair message | Contextual suggestions |

---

## 🔧 Configuration Notes

No configuration changes required. All improvements are:
- ✅ Backward compatible
- ✅ Non-breaking
- ✅ Automatically applied
- ✅ Optional debug features (don't affect normal operation)

---

## 📚 Future Enhancements

Potential follow-ups:
1. **Query caching** — Cache successful queries to avoid re-generation
2. **A/B testing** — Track which query patterns work best
3. **Smart LIKE patterns** — Learn patterns from successful historical queries
4. **Cost estimation** — Estimate query cost before execution
5. **Batch mode** — Run multiple questions efficiently
6. **SQL explain** — Show query plans for optimization insights

---

## ✨ Summary

These improvements address the main issues identified in testing:
- ✅ Eliminated redundant LIKE conditions
- ✅ Fixed NULL groups in aggregations
- ✅ Enhanced semantic keyword coverage
- ✅ Added helpful warnings and suggestions
- ✅ Created debugging and analysis tools
- ✅ Improved error recovery guidance

The system is now more robust, user-friendly, and easier to debug.
