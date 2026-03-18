# ══════════════════════════════════════════════════════════════════════════════
# Stage 1a – Base Rust + cargo-chef
# ══════════════════════════════════════════════════════════════════════════════
FROM rust:slim AS rust-chef

RUN apt-get update && apt-get install -y --no-install-recommends \
    git pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-chef --version 0.1.71

WORKDIR /build

# ══════════════════════════════════════════════════════════════════════════════
# Stage 1b – Résolution des dépendances (couche cachée)
# ══════════════════════════════════════════════════════════════════════════════
FROM rust-chef AS rust-planner

RUN git clone --depth 1 \
    https://github.com/kixelated/moq.git .

RUN cargo chef prepare --recipe-path recipe.json

# ══════════════════════════════════════════════════════════════════════════════
# Stage 1c – Compilation
# ══════════════════════════════════════════════════════════════════════════════
FROM rust-chef AS rust-builder

COPY --from=rust-planner /build/recipe.json recipe.json

# Pré-compile les dépendances (couche cachée tant que Cargo.lock ne change pas)
RUN cargo chef cook --release --recipe-path recipe.json

# Copie les sources depuis le planner
COPY --from=rust-planner /build .

# Compile les binaires
RUN cargo build --release --bin moq-relay --bin moq-cli

RUN ls -lh target/release/moq-relay target/release/moq-cli

# ══════════════════════════════════════════════════════════════════════════════
# Stage 2 – Génération des certificats TLS auto-signés (dev)
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

# Outils système
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl  \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── Binaires moq ──────────────────────────────────────────────────────────────
COPY --from=rust-builder /build/target/release/moq-relay /usr/local/bin/
COPY --from=rust-builder /build/target/release/moq-cli  /usr/local/bin/

RUN chmod +x /usr/local/bin/moq-relay /usr/local/bin/moq-cli

# ── Certificats TLS ───────────────────────────────────────────────────────────
COPY --from=cert-gen /certs /certs

# ── Dépendances Python ────────────────────────────────────────────────────────
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

# ── Code applicatif ───────────────────────────────────────────────────────────
COPY . .
#RUN cp ./main.py main.py
# ── Variables d'environnement ─────────────────────────────────────────────────
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

ENV MOQ_RELAY_BIN=/usr/local/bin/moq-relay
ENV MOQ_SUB_BIN=/usr/local/bin/moq-cli      
ENV MOQ_RELAY_ADDR=0.0.0.0:4443
ENV MOQ_RELAY_URL=https://localhost:4443
ENV MOQ_CERT_PATH=/certs/cert.pem
ENV MOQ_KEY_PATH=/certs/key.pem

ENV SEGMENT_DURATION=4
ENV SEGMENT_BUFFER_MB=32

ENV AWS_ACCESS_KEY_ID=minioadmin
ENV AWS_SECRET_ACCESS_KEY=minioadmin
ENV AWS_REGION=eu-west-1
ENV S3_BUCKET=ott-bucket
ENV ORCHESTRATOR_URL=http://orchestrator:8000

# ── Ports ─────────────────────────────────────────────────────────────────────
EXPOSE 8080
EXPOSE 4443/udp
EXPOSE 4443/tcp

# ── Healthcheck ───────────────────────────────────────────────────────────────
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -sf http://localhost:8080/health || exit 1

# ── Entrypoint ────────────────────────────────────────────────────────────────
CMD ["python", "main.py", "live_moq"]