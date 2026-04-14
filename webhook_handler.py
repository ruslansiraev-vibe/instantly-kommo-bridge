"""Business logic for processing Instantly webhook events."""

import hashlib
import logging
from dataclasses import dataclass
from typing import Optional

from kommo_client import KommoClient, KommoRateLimitError
from dedup_store import DedupStore

logger = logging.getLogger(__name__)

# Webhook events we forward to Kommo.
# reply_received is safe to include: it shares the same email_id as the later
# classified event (lead_interested, etc.), so dedup prevents double-processing.
ALLOWED_EVENTS = {"reply_received", "lead_interested", "lead_meeting_booked", "lead_out_of_office"}


@dataclass(frozen=True)
class WebhookPayload:
    """Parsed Instantly webhook payload."""

    event_type: str
    email_id: str
    lead_email: str
    first_name: str
    last_name: str
    company_name: str
    campaign_name: str
    reply_subject: str
    reply_text: str
    outbound_subject: str
    outbound_text: str
    interest_status: Optional[int]
    is_auto_reply: bool
    timestamp: str


def parse_payload(raw: dict) -> Optional[WebhookPayload]:
    """Parse raw webhook JSON into a typed payload. Returns None if unparseable."""
    try:
        event_type = raw.get("event_type", "")

        # Instantly may nest data differently per event type
        event_data = raw.get("data", raw)
        lead_data = event_data.get("lead", event_data)

        email_id = str(
            event_data.get("email_id", raw.get("email_id", ""))
            or event_data.get("reply_to_uuid", raw.get("reply_to_uuid", ""))
        )

        return WebhookPayload(
            event_type=event_type,
            email_id=email_id,
            lead_email=lead_data.get("email", event_data.get("lead_email", "")),
            first_name=lead_data.get("first_name", ""),
            last_name=lead_data.get("last_name", ""),
            company_name=lead_data.get("company_name", ""),
            campaign_name=event_data.get("campaign_name", ""),
            reply_subject=event_data.get("subject", event_data.get("reply_subject", "")),
            reply_text=event_data.get("body", event_data.get("reply_text", event_data.get("text", ""))),
            outbound_subject=event_data.get("email_subject", ""),
            outbound_text=event_data.get("email_text", ""),
            interest_status=event_data.get("lt_interest_status"),
            is_auto_reply=bool(event_data.get("is_auto_reply", False)),
            timestamp=event_data.get("timestamp", event_data.get("timestamp_email", "")),
        )
    except Exception:
        logger.exception("Failed to parse webhook payload")
        return None


def should_process(payload: WebhookPayload) -> bool:
    """Determine if this webhook event should be forwarded to Kommo."""
    if payload.event_type not in ALLOWED_EVENTS:
        logger.debug("Skipping event_type=%s", payload.event_type)
        return False

    if payload.is_auto_reply:
        logger.debug("Skipping auto-reply from %s", payload.lead_email)
        return False

    if not payload.lead_email:
        logger.warning("Skipping: no lead email in payload")
        return False

    # Skip status-only events without actual reply content
    if not payload.reply_text:
        logger.debug(
            "Skipping %s without reply text for %s",
            payload.event_type,
            payload.lead_email,
        )
        return False

    return True


@dataclass(frozen=True)
class ProcessResult:
    """Result of processing a webhook event."""

    success: bool
    contact_id: int = 0
    lead_id: int = 0
    note_id: int = 0
    error: str = ""
    deduplicated: bool = False


def process_webhook(
    payload: WebhookPayload,
    kommo: KommoClient,
    store: DedupStore,
    pipeline_id: int,
    status_id: int,
) -> ProcessResult:
    """
    Full pipeline: filter -> dedup -> find/create contact -> find/create lead -> add note.
    Returns immutable result.
    """
    dedup_key = _build_dedup_key(payload)

    # 1. Atomic dedup claim (safe under parallel workers)
    if not store.try_claim(dedup_key, payload.lead_email):
        logger.info("Already claimed/processed dedup_key=%s, skipping", dedup_key)
        return ProcessResult(success=True, deduplicated=True)

    try:
        # 2. Find or create contact
        contact = kommo.find_contact_by_email(payload.lead_email)
        if contact is None:
            contact = kommo.create_contact(
                email=payload.lead_email,
                first_name=payload.first_name,
                last_name=payload.last_name,
                company=payload.company_name,
            )

        # 3. Find or create lead
        lead = kommo.find_active_lead_by_contact(contact.id, pipeline_id=pipeline_id)
        if lead is None:
            lead_name = f"Reply from {payload.first_name} {payload.last_name}".strip()
            if lead_name == "Reply from":
                lead_name = f"Reply from {payload.lead_email}"
            lead = kommo.create_lead(
                contact_id=contact.id,
                pipeline_id=pipeline_id,
                status_id=status_id,
                name=lead_name,
                campaign_name=payload.campaign_name,
            )

        # 4. Add reply as note
        note_text = _format_note(payload)
        note_id = kommo.add_note_to_lead(lead.id, note_text)

        # 5. Finalize dedup record with created/found Kommo IDs
        store.complete_claim(
            email_id=dedup_key,
            kommo_contact_id=contact.id,
            kommo_lead_id=lead.id,
        )

        logger.info(
            "Processed: email=%s contact=%d lead=%d note=%d",
            payload.lead_email,
            contact.id,
            lead.id,
            note_id,
        )
        return ProcessResult(
            success=True,
            contact_id=contact.id,
            lead_id=lead.id,
            note_id=note_id,
        )

    except KommoRateLimitError as e:
        store.release_claim(dedup_key)
        logger.warning("Rate limited: %s", e)
        return ProcessResult(success=False, error=f"rate_limited:{e.retry_after}")
    except Exception as e:
        store.release_claim(dedup_key)
        logger.exception("Failed to process webhook for %s", payload.lead_email)
        return ProcessResult(success=False, error=str(e))


def _format_note(payload: WebhookPayload) -> str:
    """Format reply data as a Kommo note text."""
    parts = [
        f"Reply from {payload.lead_email}",
        f"Date: {payload.timestamp}",
    ]
    if payload.campaign_name:
        parts.append(f"Campaign: {payload.campaign_name}")
    if payload.reply_subject:
        parts.append(f"Subject: {payload.reply_subject}")

    if payload.outbound_subject or payload.outbound_text:
        parts.append("")
        parts.append("--- Original message ---")
        if payload.outbound_subject:
            parts.append(f"Subject: {payload.outbound_subject}")
        parts.append(payload.outbound_text or "(no text)")

    parts.append("")
    parts.append("--- Reply ---")
    parts.append(payload.reply_text or "(no text)")
    return "\n".join(parts)


def _build_dedup_key(payload: WebhookPayload) -> str:
    """
    Build stable dedup key for retries and overlapping event types.

    Prefer Instantly-provided email_id when available. If not present, use a
    deterministic fingerprint of key message attributes.
    """
    if payload.email_id:
        return payload.email_id

    fingerprint = "|".join(
        [
            payload.lead_email.strip().lower(),
            payload.timestamp.strip(),
            payload.reply_subject.strip().lower(),
            payload.reply_text.strip(),
            payload.campaign_name.strip().lower(),
        ]
    )
    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
    return f"fp:{digest}"
