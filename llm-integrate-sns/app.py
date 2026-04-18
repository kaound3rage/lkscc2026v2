import json
import httpx
import boto3
from fastapi import FastAPI, Request
from datetime import datetime, timezone, timedelta

app = FastAPI()

# ══════════════════════════════════════════════════════════════════════════════
# KONFIGURASI — ubah nilai di sini sesuai kebutuhan
# ══════════════════════════════════════════════════════════════════════════════

LLM_PROVIDER    = "groq"
GROQ_API_KEY    = "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"
GROQ_MODEL      = "llama3-8b-8192"

OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"
OLLAMA_MODEL    = "qwen2.5:1.5b"

SNS_TOPIC_ARN   = "arn:aws:sns:us-east-1:647127242402:NotificationMail"
AWS_REGION      = "us-east-1"

# Pemetaan AlarmName → CloudWatch Log Group
LIST_SNS_TOPIC_ARN = {
    "ForecastingError": "/aws/lambda/Forecasting",
    "PredictionError":  "/aws/lambda/Prediction",
}

# ── AWS clients — pakai LabRole (credentials dari instance profile EC2) ───────
sns_client  = boto3.client("sns",  region_name=AWS_REGION)
logs_client = boto3.client("logs", region_name=AWS_REGION)


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_log_group(alarm_name: str) -> str | None:
    """Petakan AlarmName ke CloudWatch Log Group via LIST_SNS_TOPIC_ARN."""
    return LIST_SNS_TOPIC_ARN.get(alarm_name)


def fetch_recent_errors(log_group: str, limit: int = 5) -> list[str]:
    """Ambil maksimal `limit` log ERROR dari 1 jam terakhir."""
    end_time   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time = int((datetime.now(timezone.utc) - timedelta(hours=1)).timestamp() * 1000)

    try:
        response = logs_client.filter_log_events(
            logGroupName=log_group,
            startTime=start_time,
            endTime=end_time,
            filterPattern="ERROR",
            limit=limit,
        )
        return [event["message"] for event in response.get("events", [])]
    except Exception as exc:
        print(f"[CloudWatch] Gagal membaca log: {exc}")
        return []


def call_llm(prompt: str) -> str:
    """Kirim prompt ke LLM (groq / ollama) dan kembalikan teks respons."""
    if LLM_PROVIDER == "groq":
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": GROQ_MODEL,
            "messages": [{"role": "user", "content": prompt}],
        }
        r = httpx.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]

    else:  # ollama
        payload = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False}
        r = httpx.post(OLLAMA_ENDPOINT, json=payload, timeout=60)
        r.raise_for_status()
        return r.json().get("response", "")


def publish_to_sns(subject: str, message: str) -> None:
    """Publish pesan ke SNS Topic."""
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],   # SNS max 100 chars untuk subject
        Message=message,
    )


# ── Webhook endpoint ──────────────────────────────────────────────────────────

@app.post("/webhook")
async def webhook(request: Request):
    body_bytes = await request.body()

    # SNS mengirim JSON dengan header x-amz-sns-message-type
    message_type = request.headers.get("x-amz-sns-message-type", "")

    try:
        body = json.loads(body_bytes)
    except json.JSONDecodeError:
        return {"status": "error", "detail": "Invalid JSON"}

    # ── 1. SubscriptionConfirmation ──────────────────────────────────────────
    if message_type == "SubscriptionConfirmation" or body.get("Type") == "SubscriptionConfirmation":
        subscribe_url = body.get("SubscribeURL")
        if subscribe_url:
            try:
                async with httpx.AsyncClient() as client:
                    await client.get(subscribe_url, timeout=10)
                print(f"[SNS] Subscription confirmed: {subscribe_url}")
            except Exception as exc:
                print(f"[SNS] Gagal konfirmasi subscription: {exc}")
        return {"status": "confirmed"}

    # ── 2. Notification ──────────────────────────────────────────────────────
    if message_type == "Notification" or body.get("Type") == "Notification":
        # Pesan dari CloudWatch Alarm ter-encode sebagai JSON string di field Message
        try:
            notification = json.loads(body.get("Message", "{}"))
        except json.JSONDecodeError:
            notification = {}

        alarm_name = notification.get("AlarmName", body.get("Subject", "UnknownAlarm"))
        print(f"[Webhook] Alarm diterima: {alarm_name}")

        # Cari log group yang berkaitan
        log_group = get_log_group(alarm_name)
        error_logs = []
        if log_group:
            error_logs = fetch_recent_errors(log_group)
            print(f"[CloudWatch] {len(error_logs)} error log ditemukan di {log_group}")
        else:
            print(f"[CloudWatch] Tidak ada log group untuk alarm: {alarm_name}")

        # Susun prompt
        logs_text = "\n".join(error_logs) if error_logs else "Tidak ada log error yang ditemukan."
        prompt = (
            "Sebagai DevOps, berikan 1 ringkasan penyebab error (Summary) "
            "dan 1 rekomendasi (Solusi) dari semua log berikut.\n\n"
            f"Log:\n{logs_text}"
        )

        # Panggil LLM
        try:
            llm_response = call_llm(prompt)
        except Exception as exc:
            llm_response = f"LLM gagal diakses: {exc}"

        print(f"[LLM] Respons: {llm_response[:200]}...")

        # Publish ke SNS
        subject = f"Resume Incident Report: {alarm_name}"
        message = (
            f"Alarm   : {alarm_name}\n"
            f"Waktu   : {datetime.now(timezone.utc).isoformat()}\n\n"
            f"--- Analisis AI ---\n{llm_response}"
        )
        try:
            publish_to_sns(subject, message)
            print(f"[SNS] Laporan dikirim: {subject}")
        except Exception as exc:
            print(f"[SNS] Gagal publish: {exc}")

        return {"status": "processed", "alarm": alarm_name}

    # ── 3. Tipe tidak dikenal ─────────────────────────────────────────────────
    return {"status": "ignored", "type": message_type}


# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}