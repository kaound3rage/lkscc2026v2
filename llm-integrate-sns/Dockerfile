# ─────────────────────────────────────────────────────────────────
# llm-integrate-sns — FastAPI webhook service
# Base image: python:3.12-slim
# ─────────────────────────────────────────────────────────────────
FROM python:3.12-slim

# Prevent .pyc files and enable unbuffered stdout/stderr for clean Docker logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (layer-cached until requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY app.py .

# Create a non-root user for security
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser
USER appuser

EXPOSE 8080

# Environment variable defaults (override via docker run --env-file or -e)
ENV PORT=8080 \
    LLM_PROVIDER=groq \
    OLLAMA_ENDPOINT=http://54.81.53.245:11434/api/generate \
    OLLAMA_MODEL=qwen2.5:1.5b \
    GROQ_API_KEY="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" \
    GROQ_MODEL=llama3-8b-8192 \
    SNS_TOPIC_ARN="arn:aws:sns:us-east-1:647127242402:NotificationMail" \
    LIST_SNS_TOPIC_ARN="{"ForecastingError":"/aws/lambda/Forecasting"}" \
    AWS_REGION=us-east-1 \
    AWS_ACCESS_KEY_ID="ASIAZNK6KC2ROLQ7GTJR" \
    AWS_SECRET_ACCESS_KEY="kQchG3NAguYFW3JMk9KGDuIdBZlm9LJdBPbHG7fy"

ENTRYPOINT ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]