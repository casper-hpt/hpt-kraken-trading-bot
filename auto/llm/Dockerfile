# syntax=docker/dockerfile:1

FROM python:3.12-slim

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
 && mkdir -p /app/theories \
 && chown -R appuser:appuser /app

# ── copy app code ──
COPY pyproject.toml ./
COPY src ./src
COPY README.md .

# ── install ──
RUN python -m pip install --upgrade pip \
 && python -m pip install .

RUN chown -R appuser:appuser /app

USER appuser

CMD ["hpt-llm"]
