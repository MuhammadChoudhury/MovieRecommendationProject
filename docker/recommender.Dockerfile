# recommender.Dockerfile

# --- Stage 1: Builder ---
FROM python:3.9-slim as builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends build-essential

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# --- Stage 2: Final Image ---
FROM python:3.9-slim

WORKDIR /app

# Copy installed Python packages from the builder stage
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages

# --- THIS IS THE CRUCIAL FIX ---
# Copy the executable scripts (like uvicorn) from the builder stage
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy your application code and trained models into the container
COPY ./service/ /app/service/
COPY ./model_registry/ /app/model_registry/

EXPOSE 8000

CMD ["uvicorn", "service.app:app", "--host", "0.0.0.0", "--port", "8000"]