"""
tests/evaluate_nl2sql.py
─────────────────────────
Golden dataset evaluator for AFM NL2SQL system.

Metrics computed:
  - Execution Accuracy (EA)   : generated SQL runs without error
  - Answer Accuracy (AA)      : row count matches gold SQL row count (±10%)
  - SQL Pattern Match (PM)    : gold_sql keywords present in generated SQL
  - Repair Rate               : % of queries that needed auto-repair

Usage:
    python tests/evaluate_nl2sql.py \
        --pg  'postgresql://afm_user:123!@localhost:5433/afm_db' \
        --model BAAI/bge-m3 \
        --llm   qwen2.5-coder:14b \
        --golden tests/golden_dataset.json \
        --out    tests/eval_results.json
"""
from __future__ import annotations
import argparse, json, logging, os, sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
logging.basicConfig(level=logging.WARNING,
                    format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ── helpers ───────────────────────────────────────────────────────────────────

def _sql_contains_all(sql: str, keywords: list[str]) -> bool:
    sql_lower = sql.lower()
    return all(kw.lower() in sql_lower for kw in keywords)


def _rows_match(gold_count: int | None, gen_count: int, tol: float = 0.10) -> bool:
    """True if generated row count is within ±tol of gold (or gold is None)."""
    if gold_count is None:
        return gen_count > 0
    if gold_count == 0:
        return gen_count == 0
    ratio = abs(gen_count - gold_count) / gold_count
    return ratio <= tol


# ── main evaluator ────────────────────────────────────────────────────────────

def run_eval(pg: str, model: str | None, llm: str, golden_path: str, out_path: str):
    os.environ["AFM_PG_DSN"] = pg
    if model:
        os.environ["AFM_EMBEDDING_MODEL_PATH"] = model

    from app.db.engine import make_engine
    from app.db.schema import ensure_schema
    from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
    from app.nl2sql.sql_generator import OllamaBackend
    from app.nl2sql.query_service import QueryService
    from sqlalchemy import text as _text

    engine = make_engine(pg)
    ensure_schema(engine)
    embedder = EmbeddingBackend(model)
    llm_backend = OllamaBackend(model=llm)
    service = QueryService.build(engine, embedder, llm_backend)

    # Load gold SQL row counts by running gold SQL against DB
    def run_gold_sql(sql: str) -> int | None:
        try:
            with engine.connect() as conn:
                rows = conn.execute(_text(sql)).fetchall()
            return len(rows)
        except Exception:
            return None

    with open(golden_path) as f:
        dataset = json.load(f)

    results = []
    metrics = {
        "total": 0, "ea_pass": 0, "aa_pass": 0,
        "pm_pass": 0, "repair": 0, "error": 0,
        "by_difficulty": {"easy": {"total":0,"ea":0,"aa":0},
                          "medium": {"total":0,"ea":0,"aa":0},
                          "hard": {"total":0,"ea":0,"aa":0}},
        "by_group": {}
    }

    for item in dataset:
        qid   = item["id"]
        q     = item["question"]
        gold  = item["gold_sql"]
        kws   = item.get("check_sql_contains", [])
        diff  = item.get("difficulty", "medium")
        group = item.get("group", "other")
        check_rows_eq  = item.get("check_rows_eq")
        check_rows_gt  = item.get("check_rows_gt")
        check_val_gt   = item.get("check_value_gt")

        print(f"\n[{qid}] {q[:70]}")

        # Run gold SQL to get expected row count
        gold_rows = run_gold_sql(gold)

        # Run NL2SQL
        t0 = time.perf_counter()
        try:
            result = service.run(q)
            elapsed = time.perf_counter() - t0
            gen_sql    = result.sql or ""
            gen_rows   = len(result.rows) if result.rows else 0
            gen_error  = result.error
            gen_repair = result.repaired
        except Exception as exc:
            elapsed    = time.perf_counter() - t0
            gen_sql    = ""
            gen_rows   = 0
            gen_error  = str(exc)
            gen_repair = False

        # ── Metric 1: Execution Accuracy (EA)
        ea = gen_error is None

        # ── Metric 2: Pattern Match (PM)
        pm = _sql_contains_all(gen_sql, kws) if kws else True

        # ── Metric 3: Answer Accuracy (AA)
        if check_rows_eq is not None:
            aa = gen_rows == check_rows_eq
        elif check_rows_gt is not None:
            aa = gen_rows > check_rows_gt
        elif gold_rows is not None:
            aa = _rows_match(gold_rows, gen_rows)
        else:
            aa = ea  # fallback: if it runs without error

        status = "✓" if (ea and pm and aa) else ("↺" if gen_repair else "✗")
        print(f"  {status} EA={ea} PM={pm} AA={aa} rows={gen_rows} "
              f"{'(repaired)' if gen_repair else ''} {elapsed:.1f}s")
        if gen_error:
            print(f"  ERROR: {gen_error}")

        # Accumulate
        metrics["total"] += 1
        if ea: metrics["ea_pass"] += 1
        if aa: metrics["aa_pass"] += 1
        if pm: metrics["pm_pass"] += 1
        if gen_repair: metrics["repair"] += 1
        if not ea: metrics["error"] += 1

        d = metrics["by_difficulty"].setdefault(diff, {"total":0,"ea":0,"aa":0})
        d["total"] += 1
        if ea: d["ea"] += 1
        if aa: d["aa"] += 1

        g = metrics["by_group"].setdefault(group, {"total":0,"ea":0,"aa":0,"pm":0})
        g["total"] += 1
        if ea: g["ea"] += 1
        if aa: g["aa"] += 1
        if pm: g["pm"] += 1

        results.append({
            "id": qid, "question": q, "difficulty": diff, "group": group,
            "gold_sql": gold, "generated_sql": gen_sql,
            "gold_rows": gold_rows, "generated_rows": gen_rows,
            "ea": ea, "pm": pm, "aa": aa, "repaired": gen_repair,
            "error": gen_error, "elapsed_s": round(elapsed, 2),
        })

    # ── Print summary ──────────────────────────────────────────────────────────
    n = metrics["total"]
    print("\n" + "═"*60)
    print(f"  TOTAL:  {n} questions")
    print(f"  EA  (execution accuracy): {metrics['ea_pass']}/{n} = {metrics['ea_pass']/n:.1%}")
    print(f"  AA  (answer accuracy):    {metrics['aa_pass']}/{n} = {metrics['aa_pass']/n:.1%}")
    print(f"  PM  (pattern match):      {metrics['pm_pass']}/{n} = {metrics['pm_pass']/n:.1%}")
    print(f"  Repair rate:              {metrics['repair']}/{n}  = {metrics['repair']/n:.1%}")
    print(f"  Error rate:               {metrics['error']}/{n}   = {metrics['error']/n:.1%}")

    print("\n  By difficulty:")
    for diff, d in metrics["by_difficulty"].items():
        if d["total"]:
            print(f"    {diff:8s}: EA={d['ea']}/{d['total']} AA={d['aa']}/{d['total']}")

    print("\n  By group:")
    for grp, g in sorted(metrics["by_group"].items()):
        print(f"    {grp:18s}: EA={g['ea']}/{g['total']} AA={g['aa']}/{g['total']} PM={g['pm']}/{g['total']}")
    print("═"*60)

    # Save results
    output = {"metrics": metrics, "results": results}
    with open(out_path, "w") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n  Full results saved to: {out_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg",     required=True)
    ap.add_argument("--model",  default=None)
    ap.add_argument("--llm",    default="qwen2.5-coder:14b")
    ap.add_argument("--golden", default="tests/golden_dataset.json")
    ap.add_argument("--out",    default="tests/eval_results.json")
    args = ap.parse_args()
    run_eval(args.pg, args.model, args.llm, args.golden, args.out)