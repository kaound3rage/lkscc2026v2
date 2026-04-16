# ─────────────────────────────────────────────
# Dockerfile — llm-integrate-sns
# Base image: python:3.12-slim
# ─────────────────────────────────────────────

FROM python:3.12-slim

# Label metadata
LABEL maintainer="devops-team"
LABEL description="FastAPI webhook service for SNS + LLM incident reporting"
LABEL version="1.0.0"

# Set environment supaya Python tidak buffer output
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Working directory
WORKDIR /app

# Install system dependencies minimal
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements terlebih dahulu (layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY app.py .

# Copy .env jika ada (opsional, bisa di-override via docker run --env-file)
COPY .env* ./

# Expose port sesuai environment variable PORT (default 8080)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8080}/health || exit 1

# Jalankan FastAPI dengan uvicorn
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1"]