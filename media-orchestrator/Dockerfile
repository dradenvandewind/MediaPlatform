FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements.txt .

RUN pip install --upgrade pip \
 && pip install --no-cache-dir --prefix=/install -r requirements.txt \
 && pip install --no-cache-dir --prefix=/install \
      pytest pytest-asyncio pytest-cov httpx fakeredis[aioredis] aiosqlite

# ── Stage test ────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS test

WORKDIR /app

COPY --from=builder /install /usr/local

COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt

# Tout sous /app
COPY app/     ./app/
COPY tests/   ./tests/
COPY pytest.ini ./pytest.ini

# __init__.py pour les packages Python
RUN touch ./app/__init__.py \
 && touch ./tests/__init__.py \
 && touch ./tests/exceptions/__init__.py

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

CMD ["pytest", "-v", "--cov=app", "--cov-report=term-missing"]

# ── Stage runtime ──────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

RUN apt-get update && apt-get install -y --no-install-recommends vim \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /install /usr/local

# Tout sous /app
COPY app/     ./app/
COPY tests/   ./tests/
COPY pytest.ini ./pytest.ini

RUN touch ./app/__init__.py \
 && touch ./tests/__init__.py \
 && touch ./tests/exceptions/__init__.py \
 && chown -R appuser:appgroup /app

USER appuser

ENV OTT_REDIS_URL=redis://redis:6379 \
    OTT_DATABASE_URL=postgresql+asyncpg://ott:ott@postgres:5432/ott \
    OTT_HOST=0.0.0.0 \
    OTT_PORT=8000 \
    OTT_LOG_LEVEL=info \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["sh", "-c", "uvicorn app.main:app --host $OTT_HOST --port $OTT_PORT --log-level $OTT_LOG_LEVEL"]