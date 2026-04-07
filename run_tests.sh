#!/bin/bash
# Run from: cd afm_final && bash run_tests.sh
# Results saved to: test_results.log

PG="${AFM_PG_DSN:-postgresql://user:pass@localhost:5432/afm}"
MODEL="${AFM_EMBEDDING_MODEL_PATH:-models/bge-m3}"
LLM_URL="http://localhost:11434"
LLM_MODEL="qwen2.5-coder:14b"
LOG="test_results.log"
PASS=0
FAIL=0

echo "AFM TEST RUN — $(date)" | tee "$LOG"

Q() {
  echo ""                                                         | tee -a "$LOG"
  echo "══════════════════════════════════════════════"           | tee -a "$LOG"
  echo "▶ QUERY: $1"                                             | tee -a "$LOG"
  echo "══════════════════════════════════════════════"           | tee -a "$LOG"

  # Run — stderr (logs/progress bars) to screen only, stdout to screen+log
  PYTHONPATH=. python -u scripts/query_cli.py \
    --pg "$PG" --model "$MODEL" \
    --llm_url "$LLM_URL" --llm_model "$LLM_MODEL" \
    "$1" 2>&1 | tee -a "$LOG"

  # Extract row count from log
  ROWS=$(grep "Rows returned" "$LOG" | tail -1 | grep -o '[0-9]*$')
  if [ -z "$ROWS" ]; then
    echo "  ⚠  NO OUTPUT / ERROR (check log)" | tee -a "$LOG"
    FAIL=$((FAIL+1))
  elif [ "$ROWS" = "0" ]; then
    echo "  ⚠  0 rows returned"               | tee -a "$LOG"
    FAIL=$((FAIL+1))
  else
    echo "  ✅ $ROWS rows"                     | tee -a "$LOG"
    PASS=$((PASS+1))
  fi
}

# --- Group 1: basic existence check ---
Q "Покажи все транзакции"
Q "Последние 10 транзакций"

# --- Group 2: direction ---
Q "Все входящие переводы"
Q "Все исходящие платежи"

# --- Group 3: amounts ---
Q "Платежи больше 5 миллионов"
Q "Топ 10 самых крупных платежей"

# --- Group 4: dates ---
Q "Транзакции за 2024 год"
Q "Транзакции за 2023 год"

# --- Group 5: semantic topics ---
Q "Платежи по займам и кредитам"
Q "Налоговые выплаты"
Q "Выплаты заработной платы"

# --- Group 6: ATM / cash ---
Q "Снятие наличных"
Q "Операции через банкоматы Halyk"
Q "Снятие наличных больше 100 тысяч"

# --- Group 7: aggregation ---
Q "Топ 10 получателей по сумме"
Q "Суммарные обороты по месяцам за 2024"

echo ""                                                             | tee -a "$LOG"
echo "╔══════════════════════════════════════════════╗"            | tee -a "$LOG"
echo "  FINAL: ✅ $PASS passed   ⚠  $FAIL failed/empty"           | tee -a "$LOG"
echo "  Full log → test_results.log"                               | tee -a "$LOG"
echo "╚══════════════════════════════════════════════╝"            | tee -a "$LOG"