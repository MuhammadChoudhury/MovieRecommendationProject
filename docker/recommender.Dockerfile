FROM python:3.9-slim AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

FROM python:3.9-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY --from=builder /usr/local /usr/local

COPY ./config.py /app/config.py

COPY ./service/ /app/service/
COPY ./model_registry/ /app/model_registry/

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD wget -qO- http://127.0.0.1:${PORT:-8080}/healthz || exit 1

CMD ["sh","-c","uvicorn service.app:app --host 0.0.0.0 --port ${PORT:-8080}"]