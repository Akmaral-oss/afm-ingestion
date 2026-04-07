from datetime import datetime, timedelta
from jose import jwt

JWT_SECRET = "dev-secret"
JWT_ALG = "HS256"

def hash_password(password: str) -> str:
    # просто возвращаем пароль как есть
    return password

def verify_password(password: str, password_hash: str) -> bool:
    return password == password_hash

def create_access_token(data: dict):
    payload = data.copy()
    payload["exp"] = datetime.utcnow() + timedelta(days=1)
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)


def decode_access_token(token: str) -> dict:
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
