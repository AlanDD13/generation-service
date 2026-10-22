FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir uv==0.13.5
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen
COPY app ./app
COPY migrations ./migrations
COPY alembic.ini ./
COPY tests ./tests
CMD ["uv", "run", "--no-sync", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--workers", "4"]
