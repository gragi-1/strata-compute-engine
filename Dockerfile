FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY control_plane ./control_plane
COPY scheduler ./scheduler
COPY worker ./worker
COPY cli ./cli
RUN pip install --no-cache-dir uv==0.9.18 && uv sync --locked --no-dev
COPY alembic.ini ./
COPY migrations ./migrations
COPY examples ./examples
RUN mkdir -p /app/data/artifacts && chown -R 65534:65534 /app/data
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER 65534:65534
EXPOSE 8000 50051
CMD ["uvicorn", "control_plane.api:app", "--host", "0.0.0.0", "--port", "8000"]
