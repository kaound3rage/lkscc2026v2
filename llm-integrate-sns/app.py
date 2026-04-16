"""
llm-integrate-sns/app.py
FastAPI webhook service untuk menerima SNS notification, membaca CloudWatch Logs,
mengirim ke LLM (Ollama atau Groq), dan mempublish hasilnya kembali ke SNS.
"""

import os
import json
import time
import logging
import httpx

import boto3
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, Request, Response
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# Konfigurasi
# ─────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="LLM Integrate SNS Webhook", version="1.0.0")

# Environment variables
LLM_PROVIDER       = os.getenv("LLM_PROVIDER", "ollama").lower()          # "ollama" | "groq"
OLLAMA_ENDPOINT    = os.getenv("OLLAMA_ENDPOINT", "http://localhost:11434/api/generate")
OLLAMA_MODEL       = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b")
GROQ_API_KEY       = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL         = os.getenv("GROQ_MODEL", "llama3-8b-8192")
SNS_TOPIC_ARN      = os.getenv("SNS_TOPIC_ARN", "")
AWS_REGION         = os.getenv("AWS_REGION", "ap-southeast-1")

# Mapping AlarmName → CloudWatch Log Group
# Format JSON: '{"ForecastingError":"/aws/lambda/Forecasting","PredictionError":"/aws/lambda/Prediction"}'
_raw_list_sns = os.getenv("LIST_SNS_TOPIC_ARN", "{}")
try:
    ALARM_TO_LOG_GROUP: dict[str, str] = json.loads(_raw_list_sns)
except json.JSONDecodeError:
    logger.warning("LIST_SNS_TOPIC_ARN bukan JSON valid, menggunakan {} sebagai default")
    ALARM_TO_LOG_GROUP = {}

# AWS clients
_boto_kwargs: dict[str, Any] = {"region_name": AWS_REGION}
if os.getenv("AWS_ACCESS_KEY_ID"):
    _boto_kwargs["aws_access_key_id"]     = os.getenv("AWS_ACCESS_KEY_ID")
    _boto_kwargs["aws_secret_access_key"] = os.getenv("AWS_SECRET_ACCESS_KEY")

logs_client = boto3.client("logs", **_boto_kwargs)
sns_client  = boto3.client("sns",  **_boto_kwargs)

# ─────────────────────────────────────────────
# Helper: Ambil log error dari CloudWatch
# ─────────────────────────────────────────────

def fetch_error_logs(log_group: str, limit: int = 5) -> list[str]:
    """
    Ambil 'limit' log yang mengandung kata 'ERROR' dalam 1 jam terakhir
    dari CloudWatch Log Group yang diberikan.
    """
    end_time   = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    start_ms = int(start_time.timestamp() * 1000)
    end_ms   = int(end_time.timestamp() * 1000)

    try:
        response = logs_client.filter_log_events(
            logGroupName  = log_group,
            startTime     = start_ms,
            endTime       = end_ms,
            filterPattern = "ERROR",
            limit         = limit,
        )
        events = response.get("events", [])
        messages = [e["message"].strip() for e in events if e.get("message")]
        logger.info(f"Ditemukan {len(messages)} log ERROR dari {log_group}")
        return messages

    except logs_client.exceptions.ResourceNotFoundException:
        logger.warning(f"Log group tidak ditemukan: {log_group}")
        return []
    except Exception as exc:
        logger.error(f"Gagal fetch log dari {log_group}: {exc}")
        return []


# ─────────────────────────────────────────────
# Helper: Kirim prompt ke LLM
# ─────────────────────────────────────────────

def build_prompt(logs: list[str]) -> str:
    joined = "\n".join(f"- {line}" for line in logs) if logs else "(tidak ada log error yang ditemukan)"
    return (
        "Sebagai DevOps, berikan 1 ringkasan penyebab error (Summary) "
        "dan 1 rekomendasi (Solusi) dari semua log berikut.\n\n"
        f"Log Error:\n{joined}"
    )


def call_ollama(prompt: str) -> str:
    payload = {
        "model":  OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
    }
    with httpx.Client(timeout=120) as client:
        resp = client.post(OLLAMA_ENDPOINT, json=payload)
        resp.raise_for_status()
        data = resp.json()
        return data.get("response", "").strip()


def call_groq(prompt: str) -> str:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type":  "application/json",
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": "Kamu adalah seorang DevOps engineer yang ahli."},
            {"role": "user",   "content": prompt},
        ],
    }
    with httpx.Client(timeout=120) as client:
        resp = client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"].strip()


def call_llm(prompt: str) -> str:
    if LLM_PROVIDER == "groq":
        logger.info("Menggunakan Groq sebagai LLM provider")
        return call_groq(prompt)
    else:
        logger.info("Menggunakan Ollama sebagai LLM provider")
        return call_ollama(prompt)


# ─────────────────────────────────────────────
# Helper: Publish hasil ke SNS
# ─────────────────────────────────────────────

def publish_to_sns(alarm_name: str, llm_response: str) -> None:
    subject = f"Resume Incident Report: {alarm_name}"
    message = (
        f"=== INCIDENT REPORT ===\n"
        f"Alarm    : {alarm_name}\n"
        f"Waktu    : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"Provider : {LLM_PROVIDER.upper()}\n\n"
        f"--- Analisis LLM ---\n"
        f"{llm_response}\n"
        f"=======================\n"
    )
    try:
        response = sns_client.publish(
            TopicArn = SNS_TOPIC_ARN,
            Subject  = subject[:100],   # SNS subject max 100 karakter
            Message  = message,
        )
        logger.info(f"Berhasil publish ke SNS. MessageId: {response['MessageId']}")
    except Exception as exc:
        logger.error(f"Gagal publish ke SNS: {exc}")
        raise


# ─────────────────────────────────────────────
# ENDPOINT: POST /webhook
# ─────────────────────────────────────────────

@app.post("/webhook")
async def webhook(request: Request) -> Response:
    """
    Menerima SNS notification:
    - SubscriptionConfirmation : auto-konfirmasi via GET ke SubscribeURL
    - Notification             : proses alarm, ambil log, panggil LLM, publish SNS
    """
    try:
        body_bytes = await request.body()
        body_text  = body_bytes.decode("utf-8")
        payload    = json.loads(body_text)
    except Exception as exc:
        logger.error(f"Gagal parse request body: {exc}")
        return Response(content="Bad Request", status_code=400)

    message_type = payload.get("Type", "")
    logger.info(f"Menerima SNS message tipe: {message_type}")

    # ── 1. SubscriptionConfirmation ──────────────────────
    if message_type == "SubscriptionConfirmation":
        subscribe_url = payload.get("SubscribeURL")
        if subscribe_url:
            try:
                with httpx.Client(timeout=30) as client:
                    resp = client.get(subscribe_url)
                    resp.raise_for_status()
                logger.info("SubscriptionConfirmation berhasil dikonfirmasi")
                return Response(content="Confirmed", status_code=200)
            except Exception as exc:
                logger.error(f"Gagal konfirmasi subscription: {exc}")
                return Response(content="Confirmation Failed", status_code=500)
        return Response(content="No SubscribeURL", status_code=400)

    # ── 2. Notification ──────────────────────────────────
    elif message_type == "Notification":
        # Parse message JSON dari SNS (CloudWatch alarm payload)
        try:
            message_body = json.loads(payload.get("Message", "{}"))
        except json.JSONDecodeError:
            message_body = {"AlarmName": payload.get("Subject", "UnknownAlarm")}

        alarm_name  = message_body.get("AlarmName", "UnknownAlarm")
        alarm_state = message_body.get("NewStateValue", "UNKNOWN")
        logger.info(f"Alarm: {alarm_name}, State: {alarm_state}")

        # Hanya proses saat state ALARM (bukan OK)
        if alarm_state == "OK":
            logger.info("State OK, tidak perlu diproses")
            return Response(content="OK state ignored", status_code=200)

        # Cari log group berdasarkan alarm name
        log_group = ALARM_TO_LOG_GROUP.get(alarm_name)
        if not log_group:
            logger.warning(f"AlarmName '{alarm_name}' tidak ada di LIST_SNS_TOPIC_ARN mapping")
            log_group = f"/aws/lambda/{alarm_name.replace('Error', '')}"
            logger.info(f"Menggunakan fallback log group: {log_group}")

        # Ambil 5 log error terbaru
        error_logs = fetch_error_logs(log_group, limit=5)

        # Buat prompt dan panggil LLM
        prompt       = build_prompt(error_logs)
        llm_response = call_llm(prompt)
        logger.info(f"Respons LLM (preview): {llm_response[:200]}...")

        # Publish hasil ke SNS
        publish_to_sns(alarm_name, llm_response)

        return Response(content="Processed", status_code=200)

    # ── 3. Tipe tidak dikenal ────────────────────────────
    else:
        logger.warning(f"Tipe SNS message tidak dikenal: {message_type}")
        return Response(content="Unknown message type", status_code=400)


# ─────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────

@app.get("/health")
def health_check():
    return {
        "status":       "healthy",
        "llm_provider": LLM_PROVIDER,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────
# Entry point (untuk development)
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)