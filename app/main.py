from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from sqladmin import Admin, ModelView
from starlette.middleware.sessions import SessionMiddleware

from sqlalchemy import text

from app.admin_auth import AdminAuth
from app.config import settings
from app.database import Base, async_session, async_engine as engine, engine as sync_engine
from app.db.schema import ensure_schema
from app.models import Project, Transaction, TransactionUploadMeta, User
from app.routers import analytics, auth, chat, projects, transactions
from app.seed import seed_admin_if_missing


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with engine.begin() as conn:
        # Diagnostic identity check: Log who the app is actually connecting as
        result = await conn.execute(text("SELECT CURRENT_USER, CURRENT_DATABASE();"))
        user, db = result.fetchone()
        print(f"INFO: Database identity: user='{user}', database='{db}'")
        
        # 1. Ensure schema exists first
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS afm;"))
        
        # 2. Map ORM models to tables
        await conn.run_sync(Base.metadata.create_all)
        
    # 3. Handle complex views, extensions, and incremental updates
    await run_in_threadpool(ensure_schema, sync_engine)

    if settings.ENABLE_SEED:
        async with async_session() as session:
            await seed_admin_if_missing(session)

    yield

    chat.close_chat_runtime()
    await engine.dispose()


app = FastAPI(
    title=settings.APP_TITLE,
    version=settings.APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(SessionMiddleware, secret_key=settings.SESSION_SECRET)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class TransactionAdmin(ModelView, model=Transaction):
    column_list = [
        Transaction.id,
        Transaction.date,
        Transaction.sender_name,
        Transaction.recipient_name,
        Transaction.amount_tenge,
        Transaction.currency,
        Transaction.category,
    ]


class TransactionUploadMetaAdmin(ModelView, model=TransactionUploadMeta):
    column_list = [
        TransactionUploadMeta.tx_id,
        TransactionUploadMeta.uploaded_by_email,
        TransactionUploadMeta.created_at,
    ]


class UserAdmin(ModelView, model=User):
    column_list = [User.id, User.email, User.role]
    column_searchable_list = [User.email]
    column_sortable_list = [User.id, User.email]


class ProjectAdmin(ModelView, model=Project):
    column_list = [Project.project_id, Project.name, Project.owner_user_id, Project.created_at]
    column_searchable_list = [Project.name]


admin = Admin(app, engine, authentication_backend=AdminAuth(secret_key=settings.SESSION_SECRET))
admin.add_view(TransactionAdmin)
admin.add_view(TransactionUploadMetaAdmin)
admin.add_view(UserAdmin)
admin.add_view(ProjectAdmin)

app.include_router(auth.router, prefix="/api/v1")
app.include_router(projects.router, prefix="/api/v1")
app.include_router(transactions.router, prefix="/api/v1")
app.include_router(analytics.router, prefix="/api/v1")
app.include_router(chat.router, prefix="/api/v1")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "afm-ingestion-api"}
