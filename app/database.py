from sqlalchemy import create_engine

from app.config import settings

engine = create_engine(
    settings.pg_dsn, 
    pool_pre_ping=True, 
    future=True,
    pool_size=2,
    max_overflow=3,
    pool_timeout=10,
    pool_recycle=300
)



    