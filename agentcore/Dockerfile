# Build context must be the repository root:
#   docker build -f agentcore/Dockerfile -t agentcore .

FROM python:3.13-slim-bookworm

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy workspace structure
COPY pyproject.toml /app/pyproject.toml
COPY uv.lock /app/uv.lock
COPY memory/ /app/memory/
COPY recommendation/ /app/recommendation/
COPY agentcore/ /app/agentcore/
COPY batch/pyproject.toml /app/batch/pyproject.toml

WORKDIR /app/agentcore

# Install dependencies
RUN uv sync --locked

ENV UV_CACHE_DIR=/tmp/uv-cache
ENV PORT=8080

CMD ["uv", "run", "--no-sync", "main.py"]
