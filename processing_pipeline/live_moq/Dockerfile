# ══════════════════════════════════════════════════════════════════════════════
# Stage 1 – Build moq-rs (Rust)
#   Compile moq-relay et moq-sub depuis les sources.
#   On utilise cargo-chef pour un cache layer optimal des dépendances.
# ══════════════════════════════════════════════════════════════════════════════
FROM rust:1.78-slim AS rust-chef

RUN cargo install cargo-chef --locked
WORKDIR /build

# ── Résolution des dépendances (couche cachée) ─────────────────────────────
FROM rust-chef AS rust-planner

RUN apt-get update && apt-get install -y --no-install-recommends \
    git pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch v0.4.0 \
    https://github.com/kixelated/moq-rs.git .

RUN cargo chef prepare --recipe-path recipe.json

# ── Compilation ────────────────────────────────────────────────────────────
FROM rust-chef AS rust-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=rust-planner /build/recipe.json recipe.json

# Pré-compile les dépendances (couche cachée tant que Cargo.lock ne change pas)
RUN cargo chef cook --release --recipe-path recipe.json

COPY --from=rust-planner /build .
RUN cargo build --release \
    --bin moq-relay \
    --bin moq-sub

# Binaires résultants
RUN ls -lh target/release/moq-relay target/release/moq-sub


# ══════════════════════════════════════════════════════════════════════════════
# Stage 2 – Génération des certificats TLS auto-signés (dev)
#   En production, monter de vrais certs via un volume ou un secret.
# ══════════════════════════════════════════════════════════════════════════════
FROM alpine:3.19 AS cert-gen

RUN apk add --no-cache openssl

RUN mkdir -p /certs && \
    openssl req -x509 -newkey ec \
        -pkeyopt ec_paramgen_curve:P-256 \
        -keyout /certs/key.pem \
        -out    /certs/cert.pem \
        -days   365 \
        -nodes  \
        -subj   "/CN=moq-relay" \
        -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"


# ══════════════════════════════════════════════════════════════════════════════
# Stage 3 – Runtime Python
# ══════════════════════════════════════════════════════════════════════════════
FROM python:3.11-slim

# Outils système + ffmpeg (pour les muxers de segments si besoin)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl  \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── Binaires moq-rs ────────────────────────────────────────────────────────
COPY --from=rust-builder \
    /build/target/release/moq-relay \
    /build/target/release/moq-sub   \
    /usr/local/bin/

RUN chmod +x /usr/local/bin/moq-relay /usr/local/bin/moq-sub

# ── Certificats TLS (auto-signés, à remplacer en prod) ────────────────────
COPY --from=cert-gen /certs /certs

# ── Dépendances Python ─────────────────────────────────────────────────────
WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
 && pip install --no-cache-dir \
      "fastapi>=0.111"                          \
      "uvicorn[standard]>=0.29"                 \
      "pydantic>=2.7"                           \
      "aioboto3>=13"                            \
      "aiohttp>=3.9"                            \
      "prometheus-fastapi-instrumentator>=6.1"  \
      "prometheus-client>=0.20"                 \
      "redis>=5.0"                              \
      pytest pytest-asyncio httpx

# ── Code applicatif ─────────────────────────────────────────────────────────
COPY . processing_pipeline/
RUN cp processing_pipeline/main.py main.py

# ── Variables d'environnement ────────────────────────────────────────────────
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Binaires moq-rs
ENV MOQ_RELAY_BIN=/usr/local/bin/moq-relay
ENV MOQ_SUB_BIN=/usr/local/bin/moq-sub
ENV MOQ_RELAY_ADDR=0.0.0.0:4443
ENV MOQ_RELAY_URL=https://localhost:4443
ENV MOQ_CERT_PATH=/certs/cert.pem
ENV MOQ_KEY_PATH=/certs/key.pem

# Tuning segments
ENV SEGMENT_DURATION=4
ENV SEGMENT_BUFFER_MB=32

# S3 / orchestrateur (surchargeables via docker-compose)
ENV AWS_ACCESS_KEY_ID=minioadmin
ENV AWS_SECRET_ACCESS_KEY=minioadmin
ENV AWS_REGION=eu-west-1
ENV S3_BUCKET=ott-bucket
ENV ORCHESTRATOR_URL=http://orchestrator:8000

# ── Ports exposés ─────────────────────────────────────────────────────────────
#  8080 – API FastAPI (HTTP)
#  4443 – moq-relay QUIC (UDP+TCP)
EXPOSE 8080
EXPOSE 4443/udp
EXPOSE 4443/tcp

# ── Healthcheck ───────────────────────────────────────────────────────────────
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -sf http://localhost:8080/health || exit 1

# ── Entrypoint ────────────────────────────────────────────────────────────────
CMD ["python", "main.py", "live_ingest"]