# ── Stage 1 : gpa build ────────────────────────────────────────
FROM python:3.11-slim AS gpac-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    cmake \
    pkg-config \
    zlib1g-dev \
    libssl-dev \
    libfreetype6-dev \
    libjpeg-dev \
    libpng-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
RUN git clone --depth 1 --branch v2.4.0 https://github.com/gpac/gpac.git

WORKDIR /build/gpac
RUN ./configure --prefix=/usr/local \
    && make -j"$(nproc)" \
    && make install

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dépendances d'abord (layer cache)
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
 && pip install --no-cache-dir \
      "fastapi>=0.111" "uvicorn[standard]>=0.29" "pydantic>=2.7" \
      "aioboto3>=13" "aiohttp>=3.9" \
      "prometheus-fastapi-instrumentator>=6.1" "prometheus-client>=0.20" \
    "redis>=5.0" \
    "aiofiles>=23.0" \ 
      pytest pytest-asyncio httpx

# Copie du code comme package processing_pipeline
COPY . processing_pipeline/

# main.py à la racine /app
RUN cp processing_pipeline/main.py main.py

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8080

CMD ["python", "main.py", "packager"]