FROM python:3.13-slim

WORKDIR /app

COPY requirements/requirements-dc.txt requirements-dc.txt
RUN pip install --no-cache-dir -r requirements-dc.txt

COPY pyproject.toml .
COPY src/ src/

RUN pip install --no-cache-dir .

COPY scripts/ scripts/
COPY crypto_watchlist.json .

ENTRYPOINT ["sh", "/app/scripts/entrypoint.sh"]
