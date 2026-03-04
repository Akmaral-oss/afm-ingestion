from __future__ import annotations
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def make_engine(pg_dsn: str) -> Engine:
    return create_engine(pg_dsn, pool_pre_ping=True, future=True)
