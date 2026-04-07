"""
db_fix/init_db.py
-----------------
Drops and re-creates the `afm` schema on the LOCAL database using the
exact GCP Cloud SQL schema (init_schema.sql).

Usage (from the project root):
    python db_fix/init_db.py            # uses AFM_PG_DSN from ../.env
    python db_fix/init_db.py --dsn "postgresql+psycopg2://postgres:1234@localhost:5432/afmdb"
"""

import argparse
import os
import sys
from pathlib import Path

# ── inherit config from parent project ──────────────────────
# Ensure the parent directory (.env location) is on sys.path
# so we can import config.py and load AFM_PG_DSN automatically.
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))
os.chdir(_project_root)  # so pydantic_settings finds ../.env

SQL_FILE = Path(__file__).resolve().parent / "init_schema.sql"


def _to_psycopg2_dsn(dsn: str) -> str:
    """Convert SQLAlchemy DSN to plain psycopg2 DSN if needed."""
    return dsn.replace("postgresql+psycopg2://", "postgresql://")


def main():
    parser = argparse.ArgumentParser(description="Re-init local DB with GCP schema")
    parser.add_argument(
        "--dsn",
        default=None,
        help="Override DSN (default: reads AFM_PG_DSN from ../.env)",
    )
    args = parser.parse_args()

    # resolve DSN
    if args.dsn:
        dsn = args.dsn
    else:
        from dotenv import dotenv_values
        env = dotenv_values(_project_root / ".env")
        dsn = env.get("AFM_PG_DSN")
        if not dsn:
            print("[init_db] ❌ AFM_PG_DSN not found in .env")
            sys.exit(1)

    dsn = _to_psycopg2_dsn(dsn)
    print(f"[init_db] Connecting to: {dsn}")

    # ── lazy import so the script can show --help without psycopg2 ──
    import psycopg2

    sql = SQL_FILE.read_text(encoding="utf-8")

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()

    print("[init_db] Running init_schema.sql ...")
    try:
        cur.execute(sql)
        print("[init_db] ✅ Schema created successfully!")
    except Exception as e:
        print(f"[init_db] ❌ Error: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

    # quick verification
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'afm' ORDER BY table_name;"
    )
    tables = [r[0] for r in cur.fetchall()]
    print(f"[init_db] Tables in afm schema ({len(tables)}):")
    for t in tables:
        print(f"  - {t}")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
