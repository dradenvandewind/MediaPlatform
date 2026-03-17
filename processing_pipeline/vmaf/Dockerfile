FROM python:3.11-slim AS builder

# ── FFmpeg + libvmaf ──────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── FFmpeg statique avec libvmaf ──────────────────────────────────────────────
# RUN apt-get update && apt-get install -y --no-install-recommends curl xz-utils \
#     && rm -rf /var/lib/apt/lists/* \
#     && curl -L https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz \
#        | tar -xJ --strip-components=1 -C /usr/local/bin ffmpeg-*/ffmpeg ffmpeg-*/ffprobe \
#     && chmod +x /usr/local/bin/ffmpeg /usr/local/bin/ffprobe

# Vérifier que libvmaf est bien disponible dans ffmpeg
RUN ffmpeg -filters 2>/dev/null | grep vmaf || \
    echo "WARNING: libvmaf not found in this ffmpeg build"

WORKDIR /app

# ── Dépendances Python ────────────────────────────────────────────────────────
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
 && pip install --no-cache-dir \
      "fastapi>=0.111" \
      "uvicorn[standard]>=0.29" \
      "pydantic>=2.7" \
      "aioboto3>=13" \
      "aiohttp>=3.9" \
      "prometheus-fastapi-instrumentator>=6.1" \
      "prometheus-client>=0.20" \
      "redis>=5.0" \
      "aiofiles>=23.0" 

# ── Code source ───────────────────────────────────────────────────────────────
COPY . processing_pipeline/
RUN cp processing_pipeline/main.py main.py

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8080

CMD ["python", "main.py", "vmaf"]
