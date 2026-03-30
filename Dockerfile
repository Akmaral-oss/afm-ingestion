FROM python:3.13-slim

# Устанавливаем uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Копируем файлы зависимостей
COPY pyproject.toml uv.lock ./

# Устанавливаем зависимости в системный Python внутри контейнера
# (так проще для Docker, чем venv)
RUN uv pip install --system --no-cache -r pyproject.toml

# Копируем остальной код
COPY . .

# Прокидываем порт
EXPOSE 8003

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8003"]