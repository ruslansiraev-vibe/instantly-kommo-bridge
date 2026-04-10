"""One-time script to register webhooks in Instantly pointing to your server."""

import os
import sys

import httpx
from dotenv import load_dotenv

load_dotenv()

INSTANTLY_API_KEY = os.environ["INSTANTLY_API_KEY"]
INSTANTLY_WEBHOOK_SECRET = os.environ.get("INSTANTLY_WEBHOOK_SECRET", "")
INSTANTLY_API_BASE = "https://api.instantly.ai/api/v2"

EVENTS_TO_REGISTER = [
    "reply_received",
    "lead_interested",
    "lead_meeting_booked",
]


def register_webhook(server_url: str, event_type: str) -> dict:
    """Register a single webhook in Instantly."""
    headers = {
        "Authorization": f"Bearer {INSTANTLY_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "target_hook_url": server_url,
        "event_type": event_type,
    }
    if INSTANTLY_WEBHOOK_SECRET:
        payload["headers"] = {"X-Webhook-Secret": INSTANTLY_WEBHOOK_SECRET}

    response = httpx.post(
        f"{INSTANTLY_API_BASE}/webhooks",
        json=payload,
        headers=headers,
        timeout=10.0,
    )
    response.raise_for_status()
    data = response.json()
    print(f"  Registered: {event_type} -> {data.get('id', 'ok')}")
    return data


def main():
    if len(sys.argv) < 2:
        print("Usage: python register_webhooks.py <YOUR_SERVER_URL>")
        print("Example: python register_webhooks.py https://your-server.com/webhook/instantly")
        sys.exit(1)

    server_url = sys.argv[1]
    print(f"Registering webhooks pointing to: {server_url}\n")

    for event in EVENTS_TO_REGISTER:
        try:
            register_webhook(server_url, event)
        except httpx.HTTPStatusError as e:
            print(f"  FAILED {event}: {e.response.status_code} {e.response.text}")

    print("\nDone. Webhooks registered.")


if __name__ == "__main__":
    main()
