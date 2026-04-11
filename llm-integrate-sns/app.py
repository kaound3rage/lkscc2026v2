import os
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import boto3
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("llm-integrate-sns")

# ── Config ────────────────────────────────────────────────────────────────────
LLM_PROVIDER      = os.getenv("LLM_PROVIDER", "groq").lower()
OLLAMA_ENDPOINT   = os.getenv("OLLAMA_ENDPOINT", "http://localhost:11434/api/generate")
OLLAMA_MODEL      = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b")
GROQ_API_KEY      = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL        = os.getenv("GROQ_MODEL", "llama3-8b-8192")
SNS_TOPIC_ARN     = os.getenv("SNS_TOPIC_ARN", "")
AWS_REGION        = os.getenv("AWS_REGION", "us-east-1")

# LIST_SNS_TOPIC_ARN maps AlarmName → CloudWatch log group
# e.g. {"ForecastingError": "/aws/lambda/Forecasting", "PredictionError": "/aws/lambda/Prediction"}
_raw_list = os.getenv("LIST_SNS_TOPIC_ARN", "{}")
try:
    ALARM_LOG_GROUP_MAP: dict[str, str] = json.loads(_raw_list)
except json.JSONDecodeError:
    logger.warning("LIST_SNS_TOPIC_ARN is not valid JSON — defaulting to {}")
    ALARM_LOG_GROUP_MAP = {}

# ── AWS clients ───────────────────────────────────────────────────────────────
def _make_boto_session() -> boto3.Session:
    return boto3.Session(
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or None,
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY") or None,
        aws_session_token=os.getenv("AWS_SESSION_TOKEN") or None,
    )

session     = _make_boto_session()
cw_logs     = session.client("logs")
sns_client  = session.client("sns")

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="LLM Integrate SNS Webhook",
    description="Receives SNS alarm notifications, analyses logs via LLM, publishes report back to SNS.",
    version="1.0.0",
)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers — CloudWatch
# ─────────────────────────────────────────────────────────────────────────────

def get_log_group_for_alarm(alarm_name: str) -> Optional[str]:
    """Return the CloudWatch log group mapped to alarm_name (case-insensitive strip)."""
    for key, log_group in ALARM_LOG_GROUP_MAP.items():
        if key.strip().lower() == alarm_name.strip().lower():
            return log_group
    return None


def fetch_recent_error_logs(log_group: str, limit: int = 5, hours: int = 1) -> list[str]:
    """
    Fetch up to `limit` log events containing 'ERROR' from the last `hours` hours
    across all log streams in `log_group`.
    Returns a list of log message strings.
    """
    now_ms   = int(time.time() * 1000)
    start_ms = now_ms - hours * 3600 * 1000

    try:
        resp = cw_logs.filter_log_events(
            logGroupName=log_group,
            startTime=start_ms,
            endTime=now_ms,
            filterPattern='"ERROR"',
            limit=limit,
        )
        events = resp.get("events", [])
        messages = [e["message"].strip() for e in events if e.get("message")]
        logger.info("Fetched %d error log(s) from %s", len(messages), log_group)
        return messages

    except cw_logs.exceptions.ResourceNotFoundException:
        logger.warning("Log group not found: %s", log_group)
        return []
    except Exception as exc:
        logger.error("CloudWatch fetch failed: %s", exc)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Helpers — LLM
# ─────────────────────────────────────────────────────────────────────────────

def build_prompt(alarm_name: str, logs: list[str]) -> str:
    log_block = "\n".join(f"- {line}" for line in logs) if logs else "- (tidak ada log error yang ditemukan)"
    return (
        f"Sebagai DevOps, berikan 1 ringkasan penyebab error (Summary) dan "
        f"1 rekomendasi (Solusi) dari semua log berikut.\n\n"
        f"Alarm: {alarm_name}\n\n"
        f"Log Error:\n{log_block}"
    )


async def call_ollama(prompt: str) -> str:
    payload = {
        "model":  OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
    }
    async with httpx.AsyncClient(timeout=60) as client:
        resp = client.post(OLLAMA_ENDPOINT, json=payload)
        resp = await resp if hasattr(resp, "__await__") else resp
        resp.raise_for_status()
        data = resp.json()
        return data.get("response", "").strip()


async def call_groq(prompt: str) -> str:
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": "Kamu adalah asisten DevOps yang membantu menganalisis log error sistem."},
            {"role": "user",   "content": prompt},
        ],
        "temperature": 0.3,
        "max_tokens":  512,
    }
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers=headers,
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"].strip()


async def call_llm(prompt: str) -> str:
    """Dispatch to the configured LLM provider."""
    logger.info("Calling LLM provider: %s", LLM_PROVIDER)
    if LLM_PROVIDER == "ollama":
        return await call_ollama(prompt)
    elif LLM_PROVIDER == "groq":
        return await call_groq(prompt)
    else:
        raise ValueError(f"Unknown LLM_PROVIDER: '{LLM_PROVIDER}'. Use 'ollama' or 'groq'.")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers — SNS
# ─────────────────────────────────────────────────────────────────────────────

def publish_to_sns(alarm_name: str, llm_response: str, logs: list[str]) -> str:
    """Publish the LLM analysis report to SNS. Returns the MessageId."""
    subject = f"Resume Incident Report: {alarm_name}"

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    log_section = "\n".join(f"  {i+1}. {line}" for i, line in enumerate(logs)) if logs else "  (tidak ada log)"

    message = (
        f"=== INCIDENT REPORT ===\n"
        f"Alarm    : {alarm_name}\n"
        f"Timestamp: {timestamp}\n"
        f"Provider : {LLM_PROVIDER.upper()}\n\n"
        f"--- LOG ERROR (5 terbaru) ---\n{log_section}\n\n"
        f"--- ANALISIS LLM ---\n{llm_response}\n"
        f"=======================\n"
    )

    resp = sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],   # SNS subject max 100 chars
        Message=message,
    )
    message_id = resp["MessageId"]
    logger.info("SNS publish success — MessageId: %s", message_id)
    return message_id


# ─────────────────────────────────────────────────────────────────────────────
# Webhook endpoint
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/webhook")
async def webhook(request: Request):
    """
    Handles two SNS message types:
    - SubscriptionConfirmation : auto-confirms via GET to SubscribeURL
    - Notification             : processes alarm, fetches logs, calls LLM, publishes to SNS
    """
    # ── Parse body ────────────────────────────────────────────────────────────
    body_bytes = await request.body()
    try:
        body = json.loads(body_bytes)
    except json.JSONDecodeError:
        logger.error("Invalid JSON body received")
        raise HTTPException(status_code=400, detail="Request body must be valid JSON")

    message_type = body.get("Type", "")
    logger.info("Received SNS message type: %s", message_type)

    # ── 1. SubscriptionConfirmation ───────────────────────────────────────────
    if message_type == "SubscriptionConfirmation":
        subscribe_url = body.get("SubscribeURL")
        if not subscribe_url:
            raise HTTPException(status_code=400, detail="SubscribeURL missing in SubscriptionConfirmation")

        logger.info("Confirming SNS subscription via: %s", subscribe_url)
        async with httpx.AsyncClient(timeout=15) as client:
            confirm_resp = await client.get(subscribe_url)
            confirm_resp.raise_for_status()

        logger.info("SNS subscription confirmed successfully")
        return JSONResponse({"status": "confirmed"})

    # ── 2. Notification ───────────────────────────────────────────────────────
    elif message_type == "Notification":
        raw_message = body.get("Message", "")

        # SNS CloudWatch alarm notifications wrap data as a JSON string inside Message
        try:
            message_data = json.loads(raw_message)
        except (json.JSONDecodeError, TypeError):
            message_data = {}

        alarm_name = (
            message_data.get("AlarmName")
            or body.get("Subject", "UnknownAlarm")
        )
        logger.info("Processing alarm notification: %s", alarm_name)

        # Step 1 — Resolve log group
        log_group = get_log_group_for_alarm(alarm_name)
        if not log_group:
            logger.warning(
                "No log group mapping found for alarm '%s'. "
                "Available mappings: %s",
                alarm_name, list(ALARM_LOG_GROUP_MAP.keys())
            )

        # Step 2 — Fetch recent error logs (empty list if no mapping)
        logs = fetch_recent_error_logs(log_group) if log_group else []

        # Step 3 — Build prompt and call LLM
        prompt = build_prompt(alarm_name, logs)
        logger.info("Sending %d log(s) to LLM (%s)", len(logs), LLM_PROVIDER)

        try:
            llm_result = await call_llm(prompt)
        except httpx.HTTPStatusError as exc:
            logger.error("LLM HTTP error: %s — %s", exc.response.status_code, exc.response.text)
            raise HTTPException(status_code=502, detail=f"LLM provider error: {exc.response.status_code}")
        except Exception as exc:
            logger.error("LLM call failed: %s", exc)
            raise HTTPException(status_code=502, detail=f"LLM call failed: {str(exc)}")

        logger.info("LLM response received (%d chars)", len(llm_result))

        # Step 4 — Publish to SNS
        if not SNS_TOPIC_ARN:
            logger.warning("SNS_TOPIC_ARN is not set — skipping publish")
            return JSONResponse({
                "status":     "processed",
                "alarm":      alarm_name,
                "log_count":  len(logs),
                "llm_result": llm_result,
                "sns":        "skipped (SNS_TOPIC_ARN not set)",
            })

        try:
            message_id = publish_to_sns(alarm_name, llm_result, logs)
        except Exception as exc:
            logger.error("SNS publish failed: %s", exc)
            raise HTTPException(status_code=502, detail=f"SNS publish failed: {str(exc)}")

        return JSONResponse({
            "status":      "published",
            "alarm":       alarm_name,
            "log_group":   log_group or "(not mapped)",
            "log_count":   len(logs),
            "llm_provider": LLM_PROVIDER,
            "sns_message_id": message_id,
        })

    # ── Unknown type ──────────────────────────────────────────────────────────
    else:
        logger.warning("Unhandled SNS message type: %s", message_type)
        return JSONResponse({"status": "ignored", "type": message_type})


# ─────────────────────────────────────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status":       "ok",
        "llm_provider": LLM_PROVIDER,
        "sns_topic":    SNS_TOPIC_ARN or "(not set)",
        "alarm_map":    list(ALARM_LOG_GROUP_MAP.keys()),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    logger.info("Starting llm-integrate-sns on port %d", port)
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)