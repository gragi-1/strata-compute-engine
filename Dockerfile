FROM python:3.12-slim-trixie@sha256:dddfd7e07f9d15aeeca61529320492139d21cac7f0070c00609243e51e4e0016
RUN apt-get update && apt-get upgrade --no-install-recommends -y \
    && apt-get install --no-install-recommends -y postgresql-client-17 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN pip install --no-cache-dir uv==0.9.18 && uv sync --locked --no-dev --no-install-project
COPY control_plane ./control_plane
COPY scheduler ./scheduler
COPY worker ./worker
COPY cli ./cli
COPY strata_sdk ./strata_sdk
COPY alembic.ini ./
COPY migrations ./migrations
RUN uv sync --locked --no-dev --no-editable
COPY examples ./examples
RUN mkdir -p /app/data/artifacts /app/data/backups && chown -R 65534:65534 /app/data
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER 65534:65534
EXPOSE 8000 50051
CMD ["uvicorn", "control_plane.api:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
