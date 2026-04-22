# syntax=docker/dockerfile:1

FROM python:3.13-slim

# ── env ──
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# ── system deps ──
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Non-root user
RUN useradd -m -u 10001 appuser \
 && chown -R appuser:appuser /app \
 && mkdir -p /data && chown appuser:appuser /data \
 && mkdir -p /app/src/data && chown appuser:appuser /app/src/data

# ── install deps (cached layer) ──
COPY requirements/requirements-t.txt ./requirements/requirements-t.txt
COPY pyproject.toml ./

RUN python -m pip install --upgrade pip \
 && python -m pip install -r requirements/requirements-t.txt

# ── copy app code ──
COPY src ./src
COPY scripts ./scripts
COPY requirements ./requirements

RUN chown -R appuser:appuser /app

USER appuser

# ── entrypoint & command ──
ENTRYPOINT ["./scripts/entrypoint.sh"]
CMD ["python", "-m", "src.main"]
