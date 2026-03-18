from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from app.config import settings

# 1. Sync engine (for Streamlit and nl2sql)
engine = create_engine(
    settings.sync_pg_dsn,
    pool_pre_ping=True,
    future=True,
    pool_size=2,          # Keep maximum 2 connections open persistently
    max_overflow=3,       # Allow up to 3 extra connections during spikes (Total: 5 max)
    pool_timeout=10,      # Give up quickly if no slots are available
    pool_recycle=300,     # Recycle connections every 5 minutes (prevent cloud drops)
)

# 2. Async engine (for FastAPI added by Akberen)
async_url = getattr(settings, "DATABASE_URL", getattr(settings, "pg_dsn", "postgresql+asyncpg://..."))
if async_url.startswith("postgresql://"):
    async_url = async_url.replace("postgresql://", "postgresql+asyncpg://")

async_engine = create_async_engine(async_url, echo=False)
async_session = async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

async def get_db() -> AsyncSession:
    async with async_session() as session:
        yield session


    