from sqlalchemy import create_engine

from app.config import settings

engine = create_engine(settings.pg_dsn, pool_pre_ping=True, future=True)



    