FROM python:3.12-slim-trixie@sha256:dddfd7e07f9d15aeeca61529320492139d21cac7f0070c00609243e51e4e0016
RUN apt-get update && apt-get upgrade --no-install-recommends -y \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir numpy==2.5.3 pyarrow==23.0.1 && mkdir /output && chmod 1777 /output
COPY main.py /app/main.py
ENV OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
USER 65534:65534
WORKDIR /app
CMD ["python", "main.py", "monte-carlo"]
