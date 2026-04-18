FROM python:3.13-slim

WORKDIR /app

COPY requirements/requirements-ss.txt requirements-ss.txt
RUN pip install --no-cache-dir -r requirements-ss.txt

COPY pyproject.toml .
COPY src/ src/

RUN pip install --no-cache-dir .

COPY scripts/ scripts/

ENTRYPOINT ["sh", "/app/scripts/entrypoint.sh"]
