"""
Auth endpoints:
POST /api/v1/auth/login
POST /api/v1/auth/register/send-code
POST /api/v1/auth/register/confirm
POST /api/v1/auth/register
POST /api/v1/auth/logout
"""

from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..database import get_db
from ..models import User
from ..project_context import ensure_user_active_project
from ..schemas import (
    ErrorResponse,
    LoginRequest,
    LoginResponse,
    MessageResponse,
    RegisterRequest,
    UserOut,
)
from ..security import create_access_token, hash_password, verify_password
from app.exceptions import (
    UserALreadyExistsException, 
    IncorrectEmailOrPasswordException, 
    InvalidEmailException,
    InvalidFieldException
)

router = APIRouter(prefix="/auth", tags=["Auth"])


def _user_to_out(user: User) -> UserOut:
    name = user.email.split("@")[0] if "@" in user.email else user.email
    return UserOut(
        id=user.id,
        email=user.email,
        name=name,
        role=user.role,
        active_project_id=user.active_project_id,
    )


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _contains_control_chars(value: str) -> bool:
    return any(ord(ch) < 32 for ch in value)


def _normalize_email(email: str) -> str:
    value = email.strip().lower()
    if not value or len(value) > 254 or _contains_control_chars(value):
        raise InvalidEmailException
    return value


def _validate_secret_field(value: str, field_name: str, *, min_length: int = 1, max_length: int = 256) -> str:
    if value is None:
        raise InvalidFieldException(field_name)

    normalized = value.strip()
    if len(normalized) < min_length or len(normalized) > max_length or _contains_control_chars(normalized):
        raise InvalidFieldException(field_name)
    
    return normalized


@router.post("/login", response_model=LoginResponse, responses={401: {"model": ErrorResponse}})
async def login(body: LoginRequest, db: AsyncSession = Depends(get_db)):
    email = _normalize_email(body.email)
    password = _validate_secret_field(body.password, "password")
    result = await db.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()

    if not user or not verify_password(password, user.password_hash):
        raise IncorrectEmailOrPasswordException

    await ensure_user_active_project(db, user)
    await db.commit()
    await db.refresh(user)
    token = create_access_token({"sub": str(user.id), "email": user.email, "role": user.role})
    return LoginResponse(token=token, user=_user_to_out(user))


@router.post("/register", response_model=LoginResponse, responses={409: {"model": ErrorResponse}})
async def register(body: RegisterRequest, db: AsyncSession = Depends(get_db)):
    email = _normalize_email(body.email)
    password = _validate_secret_field(body.password, "password", min_length=6)

    existing = await db.execute(select(User).where(User.email == email))
    if existing.scalar_one_or_none():
        raise UserALreadyExistsException

    password_hash = hash_password(password)
    user = User(email=email, password_hash=password_hash, role="user")
    
    db.add(user)
    await db.flush()
    await ensure_user_active_project(db, user)
    await db.commit()
    await db.refresh(user)

    token = create_access_token({"sub": str(user.id), "email": user.email, "role": user.role})
    return LoginResponse(token=token, user=_user_to_out(user))


@router.post("/logout", response_model=MessageResponse)
async def logout():
    return MessageResponse(message="Logged out successfully")
