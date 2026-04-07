from __future__ import annotations
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def make_engine(pg_dsn: str) -> Engine:
    """
    Create SQLAlchemy engine from a DSN string.
    Accepts both short form:  postgresql://user:pass@host:port/db
    and full form:            postgresql+psycopg2://user:pass@host:port/db
    The short form is auto-upgraded to psycopg2 dialect.
    """
    if pg_dsn.startswith("postgresql://") and "+psycopg2" not in pg_dsn:
        pg_dsn = pg_dsn.replace("postgresql://", "postgresql+psycopg2://", 1)
    return create_engine(pg_dsn, pool_pre_ping=True, future=True)
