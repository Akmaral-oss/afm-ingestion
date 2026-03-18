from __future__ import annotations
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def make_engine(pg_dsn: str) -> Engine:
    # The ingestion pipeline is short-lived and sequential, so we keep its
    # pool tiny to avoid exhausting Postgres connection slots.
    return create_engine(
        pg_dsn,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        future=True,
    )
