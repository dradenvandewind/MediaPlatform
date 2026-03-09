# ─────────────────────────────────────────────
# Stage 1 – builder
# ─────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build


COPY requirements.txt .

RUN pip install --upgrade pip \
 && pip install --no-cache-dir --prefix=/install -r requirements.txt


# ─────────────────────────────────────────────
# Stage 2 – runtime 
# ─────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# Create a non-root user
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

WORKDIR /app

# Retrieve packages installed in the builder
COPY --from=builder /install /usr/local

# Copy the source code
COPY app/ ./app/

# Non-root user membership
RUN chown -R appuser:appgroup /app

USER appuser

# Default environment variables (overridable via docker run -e)
ENV OTT_REDIS_URL=redis://redis:6379 \
    OTT_HOST=0.0.0.0 \
    OTT_PORT=8000 \
    OTT_LOG_LEVEL=info \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["sh", "-c", "uvicorn app.main:app --host $OTT_HOST --port $OTT_PORT --log-level $OTT_LOG_LEVEL"]