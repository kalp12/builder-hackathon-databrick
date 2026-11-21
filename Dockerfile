FROM python:3.11-slim

RUN pip install --no-cache-dir duckdb==1.2.1

WORKDIR /app

COPY process.py .

ENV DATA_DIR=/data
ENV OUTPUT_DIR=/output
ENV SCRATCH_DIR=/scratch

CMD ["python", "process.py"]