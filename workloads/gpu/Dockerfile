FROM python:3.12-slim-trixie@sha256:dddfd7e07f9d15aeeca61529320492139d21cac7f0070c00609243e51e4e0016
RUN apt-get update && apt-get upgrade --no-install-recommends -y \
    && rm -rf /var/lib/apt/lists/* && mkdir /output && chmod 1777 /output
COPY workloads/gpu/main.py /opt/strata-gpu-smoke.py
ENV PYTHONUNBUFFERED=1 NVIDIA_DRIVER_CAPABILITIES=compute,utility
ENTRYPOINT ["python", "/opt/strata-gpu-smoke.py"]
CMD ["1024", "0"]
