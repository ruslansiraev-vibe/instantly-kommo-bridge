"""FastAPI server receiving Instantly webhooks and forwarding to Kommo."""

import hashlib
import hmac
import logging

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

from config import Config
from kommo_client import KommoClient
from dedup_store import DedupStore
from webhook_handler import (
    parse_payload,
    should_process,
    process_webhook,
)

# --- Bootstrap ---

config = Config.from_env()

logging.basicConfig(
    level=getattr(logging, config.log_level),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

kommo = KommoClient(subdomain=config.kommo_subdomain, token=config.kommo_token)
store = DedupStore(db_path=config.db_path)
app = FastAPI(title="Instantly-Kommo Bridge", version="1.0.0")


# --- Webhook secret verification ---


def verify_signature(payload_bytes: bytes, signature: str, secret: str) -> bool:
    """Verify HMAC-SHA256 signature from Instantly webhook."""
    expected = hmac.new(
        secret.encode(), payload_bytes, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


# --- Routes ---


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.post("/webhook/instantly")
async def receive_webhook(request: Request):
    """
    Receive Instantly webhook, validate, filter, and forward to Kommo.
    Returns 200 quickly (Instantly expects fast response).
    """
    body = await request.body()

    # Verify webhook signature if header present
    signature = request.headers.get("X-Webhook-Secret", "")
    if signature and config.instantly_webhook_secret:
        if not verify_signature(body, signature, config.instantly_webhook_secret):
            logger.warning("Invalid webhook signature")
            raise HTTPException(status_code=401, detail="Invalid signature")

    raw = await request.json()
    logger.debug("Received webhook: %s", raw)

    # Parse
    payload = parse_payload(raw)
    if payload is None:
        return JSONResponse({"status": "parse_error"}, status_code=400)

    # Filter
    if not should_process(payload):
        return JSONResponse({"status": "filtered"})

    # Process
    result = process_webhook(
        payload=payload,
        kommo=kommo,
        store=store,
        pipeline_id=config.kommo_pipeline_id,
        status_id=config.kommo_pipeline_status_id,
    )

    if result.success:
        return JSONResponse({
            "status": "ok",
            "contact_id": result.contact_id,
            "lead_id": result.lead_id,
        })

    if result.error.startswith("rate_limited"):
        return JSONResponse({"status": "retry"}, status_code=503)

    return JSONResponse({"status": "error", "detail": result.error}, status_code=500)
