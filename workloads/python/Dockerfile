FROM python:3.12-slim
RUN pip install --no-cache-dir numpy==2.3.1 && mkdir /output && chmod 1777 /output
COPY main.py /app/main.py
ENV OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
USER 65534:65534
WORKDIR /app
CMD ["python", "main.py", "monte-carlo"]
