"""
Auth endpoints:
POST /api/v1/auth/login
POST /api/v1/auth/register/send-code
POST /api/v1/auth/register/confirm
POST /api/v1/auth/register
POST /api/v1/auth/logout
"""

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..database import get_db
from ..email_service import generate_verification_code, send_registration_code
from ..models import PendingRegistration, User
from ..schemas import (
    ErrorResponse,
    LoginRequest,
    LoginResponse,
    MessageResponse,
    RegisterConfirmRequest,
    RegisterRequest,
    RegisterSendCodeRequest,
    UserOut,
)
from ..security import create_access_token, hash_password, verify_password

router = APIRouter(prefix="/auth", tags=["Auth"])


def _user_to_out(user: User) -> UserOut:
    name = user.email.split("@")[0] if "@" in user.email else user.email
    return UserOut(id=user.id, email=user.email, name=name)


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _contains_control_chars(value: str) -> bool:
    return any(ord(ch) < 32 for ch in value)


def _normalize_email(email: str) -> str:
    value = email.strip().lower()
    if not value or len(value) > 254 or _contains_control_chars(value):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid email",
        )
    return value


def _validate_secret_field(value: str, field_name: str, *, min_length: int = 1, max_length: int = 256) -> str:
    if value is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid {field_name}",
        )

    normalized = value.strip()
    if len(normalized) < min_length or len(normalized) > max_length or _contains_control_chars(normalized):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid {field_name}",
        )
    return normalized


@router.post("/login", response_model=LoginResponse, responses={401: {"model": ErrorResponse}})
async def login(body: LoginRequest, db: AsyncSession = Depends(get_db)):
    email = _normalize_email(body.email)
    password = _validate_secret_field(body.password, "password")
    result = await db.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()

    if not user or not verify_password(password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    token = create_access_token({"sub": str(user.id), "email": user.email, "role": user.role})
    return LoginResponse(token=token, user=_user_to_out(user))


@router.post("/register/send-code", response_model=MessageResponse, responses={409: {"model": ErrorResponse}})
async def register_send_code(body: RegisterSendCodeRequest, db: AsyncSession = Depends(get_db)):
    email = _normalize_email(body.email)
    password = _validate_secret_field(body.password, "password", min_length=6)

    existing = await db.execute(select(User).where(User.email == email))
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User with this email already exists",
        )

    code = generate_verification_code()
    now = _utc_now_naive()
    expires_at = now + timedelta(minutes=settings.EMAIL_CODE_TTL_MINUTES)
    password_hash = hash_password(password)

    pending_result = await db.execute(select(PendingRegistration).where(PendingRegistration.email == email))
    pending = pending_result.scalar_one_or_none()
    if pending:
        pending.password_hash = password_hash
        pending.verification_code = code
        pending.expires_at = expires_at
        pending.created_at = now
    else:
        db.add(
            PendingRegistration(
                email=email,
                password_hash=password_hash,
                verification_code=code,
                expires_at=expires_at,
                created_at=now,
            )
        )

    send_registration_code(email, code)
    await db.commit()
    return MessageResponse(message="Verification code sent")


@router.post(
    "/register/confirm",
    response_model=LoginResponse,
    responses={400: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
)
async def register_confirm(body: RegisterConfirmRequest, db: AsyncSession = Depends(get_db)):
    email = _normalize_email(body.email)
    code = _validate_secret_field(body.code, "verification code", min_length=4, max_length=6)

    existing = await db.execute(select(User).where(User.email == email))
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User with this email already exists",
        )

    pending_result = await db.execute(select(PendingRegistration).where(PendingRegistration.email == email))
    pending = pending_result.scalar_one_or_none()
    if not pending:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Verification code was not requested",
        )

    now = _utc_now_naive()
    if pending.expires_at < now:
        await db.delete(pending)
        await db.commit()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Verification code expired",
        )

    if pending.verification_code != code:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid verification code",
        )

    user = User(email=email, password_hash=pending.password_hash, role="user")
    db.add(user)
    await db.flush()
    await db.delete(pending)
    await db.commit()
    await db.refresh(user)

    token = create_access_token({"sub": str(user.id), "email": user.email, "role": user.role})
    return LoginResponse(token=token, user=_user_to_out(user))


@router.post("/register", response_model=MessageResponse)
async def register_legacy(_: RegisterRequest):
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Use /auth/register/send-code and /auth/register/confirm",
    )


@router.post("/logout", response_model=MessageResponse)
async def logout():
    return MessageResponse(message="Logged out successfully")
