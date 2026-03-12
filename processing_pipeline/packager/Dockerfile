# ── Stage 1 : gpa build ────────────────────────────────────────
FROM python:3.11-slim AS gpac-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git build-essential cmake pkg-config \
    zlib1g-dev libssl-dev libfreetype6-dev libjpeg-dev libpng-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
RUN git clone --depth 1 --branch v2.4.0 https://github.com/gpac/gpac.git

WORKDIR /build/gpac
RUN ./configure --prefix=/usr/local \
    && make -j"$(nproc)" -C src \
    && make -C applications/mp4box \
    && cp bin/gcc/MP4Box /usr/local/bin/MP4Box

# ── Stage 2 : last image ────────────────────────────────────────────────
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg curl libssl3 zlib1g libfreetype6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=gpac-builder /usr/local/bin/MP4Box /usr/local/bin/MP4Box

WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -e ".[dev]"

COPY . .

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8080

CMD ["python", "main.py", "packager"]
