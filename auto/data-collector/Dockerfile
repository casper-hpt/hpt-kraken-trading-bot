FROM python:3.12-slim

WORKDIR /app

COPY requirements/requirements-cdc.txt requirements-cdc.txt
RUN pip install --no-cache-dir -r requirements-cdc.txt

COPY pyproject.toml .
COPY src/ src/

RUN pip install --no-cache-dir .

COPY scripts/ scripts/
COPY crypto_watchlist.json .

ENTRYPOINT ["sh", "/app/scripts/entrypoint.sh"]
