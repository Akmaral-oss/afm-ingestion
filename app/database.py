from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from app.config import settings


def _build_async_url() -> str:
    """
    Keep compatibility with asyncpg.

    Sync DSNs use ``sslmode=...`` for psycopg2, while asyncpg expects ``ssl=...``.
    Prefer DATABASE_URL when it is already async, otherwise convert the sync DSN.
    """
    if settings.DATABASE_URL.startswith("postgresql+asyncpg://"):
        return settings.DATABASE_URL

    dsn = settings.PG_DSN or settings.sync_pg_dsn
    parts = urlsplit(dsn)
    query_items = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        if key == "sslmode":
            key = "ssl"
        query_items.append((key, value))

    scheme = parts.scheme
    if scheme == "postgresql+psycopg2":
        scheme = "postgresql+asyncpg"
    elif scheme == "postgresql":
        scheme = "postgresql+asyncpg"

    return urlunsplit((
        scheme,
        parts.netloc,
        parts.path,
        urlencode(query_items),
        parts.fragment,
    ))


# 1. Sync engine (for Streamlit and nl2sql)
engine = create_engine(
    settings.sync_pg_dsn,
    pool_pre_ping=settings.DB_POOL_PRE_PING,
    future=True,
    pool_size=max(settings.DB_POOL_SIZE, 2),
    max_overflow=max(settings.DB_MAX_OVERFLOW, 3),
    pool_timeout=min(settings.DB_POOL_TIMEOUT, 10),
    pool_recycle=min(settings.DB_POOL_RECYCLE, 300),
)

# 2. Async engine (for FastAPI)
async_engine = create_async_engine(
    _build_async_url(),
    echo=False,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_timeout=settings.DB_POOL_TIMEOUT,
    pool_recycle=settings.DB_POOL_RECYCLE,
    pool_pre_ping=settings.DB_POOL_PRE_PING,
)
async_session = async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

async def get_db():
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()

    
