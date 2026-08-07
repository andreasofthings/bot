FROM python:3.11-slim

# Copy the uv binary from the official image
COPY --from=ghcr.io/astral-sh/uv:0.2.20 /uv /uvx /bin/

# Set env settings to use the virtualenv automatically
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

# Install build tools if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Create secure system user/group
RUN groupadd -g 10001 botgroup && \
    useradd -r -m -u 10001 -g botgroup botuser && \
    mkdir -p /app/data && \
    chown -R botuser:botgroup /app

USER botuser

# Copy pyproject.toml to install dependencies
COPY --chown=botuser:botgroup pyproject.toml ./

# Synchronize dependencies (creates /app/.venv) without installing the local project
RUN uv sync --no-install --no-dev --no-cache

# Copy source code
COPY --chown=botuser:botgroup src/ ./src

EXPOSE 8080

CMD ["python", "-m", "src.main"]
