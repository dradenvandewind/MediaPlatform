FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements.txt .

RUN pip install --upgrade pip \
 && pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage test ────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS test

WORKDIR /app

# Récupère les deps de prod déjà installées
COPY --from=builder /install /usr/local

# Installe les deps de dev (pytest, coverage, httpx…)
COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt

# Copie le code source + les tests
COPY app/ ./app/
COPY tests/ ./tests/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

CMD ["pytest", "-v", "--cov=app", "--cov-report=term-missing"]

# ── Stage runtime ──────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

WORKDIR /app

COPY --from=builder /install /usr/local

COPY app/ ./app/

RUN chown -R appuser:appgroup /app

USER appuser

ENV OTT_REDIS_URL=redis://redis:6379 \
    OTT_DATABASE_URL=postgresql+asyncpg://ott:ott@postgres:5432/ott \
    OTT_HOST=0.0.0.0 \
    OTT_PORT=8000 \
    OTT_LOG_LEVEL=info \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["sh", "-c", "uvicorn app.main:app --host $OTT_HOST --port $OTT_PORT --log-level $OTT_LOG_LEVEL"]