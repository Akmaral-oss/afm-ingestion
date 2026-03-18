from sqladmin.authentication import AuthenticationBackend
from starlette.requests import Request
from sqlalchemy import select

from app.database import async_session
from app.models import User
from app.security import verify_password

class AdminAuth(AuthenticationBackend):
    async def login(self, request: Request) -> bool:
        form = await request.form()
        email = form.get("username") or form.get("email")
        password = form.get("password")

        async with async_session() as session:
            res = await session.execute(select(User).where(User.email == email))
            user = res.scalar_one_or_none()

        if not user or user.role != "admin":
            return False
        if not verify_password(password, user.password_hash):
            return False

        request.session["admin_role"] = "admin"
        request.session["admin_email"] = user.email
        return True

    async def logout(self, request: Request) -> bool:
        request.session.clear()
        return True

    async def authenticate(self, request: Request) -> bool:
        print("SESSION:", request.session)
        return request.session.get("admin_role") == "admin"