"""Kommo CRM API v4 client for contacts, leads, and notes."""

import logging
from dataclasses import dataclass
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

KOMMO_API_TIMEOUT = 10.0


@dataclass(frozen=True)
class KommoContact:
    id: int
    name: str


@dataclass(frozen=True)
class KommoLead:
    id: int
    name: str


class KommoClient:
    """Thin wrapper around Kommo REST API v4."""

    def __init__(self, subdomain: str, token: str) -> None:
        self._base_url = f"https://{subdomain}.kommo.com/api/v4"
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _request(
        self, method: str, path: str, **kwargs
    ) -> Optional[dict]:
        """Make an HTTP request to Kommo API. Returns None on 204/404."""
        url = f"{self._base_url}{path}"
        with httpx.Client(timeout=KOMMO_API_TIMEOUT) as client:
            response = client.request(
                method, url, headers=self._headers, **kwargs
            )

        if response.status_code in (204, 404):
            return None

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "1")
            logger.warning("Kommo rate limit hit, retry after %s sec", retry_after)
            raise KommoRateLimitError(retry_after=int(retry_after))

        response.raise_for_status()
        return response.json()

    # --- Contacts ---

    def find_contact_by_email(self, email: str) -> Optional[KommoContact]:
        """Search for a contact by email. Returns first match or None."""
        data = self._request("GET", "/contacts", params={"query": email, "limit": 1})
        if not data:
            return None

        contacts = data.get("_embedded", {}).get("contacts", [])
        if not contacts:
            return None

        c = contacts[0]
        return KommoContact(id=c["id"], name=c.get("name", ""))

    def create_contact(
        self,
        email: str,
        first_name: str = "",
        last_name: str = "",
        company: str = "",
    ) -> KommoContact:
        """Create a new contact in Kommo. Returns created contact."""
        name = f"{first_name} {last_name}".strip() or email
        payload = [
            {
                "name": name,
                "custom_fields_values": [
                    {
                        "field_code": "EMAIL",
                        "values": [{"value": email, "enum_code": "WORK"}],
                    }
                ],
                "tags_ids": [],
                "_embedded": {
                    "tags": [
                        {"name": "Instantly"},
                        {"name": "Auto-import"},
                    ]
                },
            }
        ]

        if company:
            payload[0]["custom_fields_values"].append(
                {
                    "field_code": "COMPANY",
                    "values": [{"value": company}],
                }
            )

        data = self._request("POST", "/contacts", json=payload)
        created = data["_embedded"]["contacts"][0]
        logger.info("Created Kommo contact id=%d for %s", created["id"], email)
        return KommoContact(id=created["id"], name=name)

    # --- Leads (deals) ---

    def find_active_lead_by_contact(
        self, contact_id: int
    ) -> Optional[KommoLead]:
        """Find an active (open) lead linked to a contact."""
        data = self._request(
            "GET",
            "/leads",
            params={
                "filter[contacts_id]": contact_id,
                "limit": 1,
                "order[updated_at]": "desc",
            },
        )
        if not data:
            return None

        leads = data.get("_embedded", {}).get("leads", [])
        # Skip closed leads (status_id 142 = won, 143 = lost in Kommo)
        active = [
            lead
            for lead in leads
            if lead.get("status_id") not in (142, 143)
        ]
        if not active:
            return None

        lead = active[0]
        return KommoLead(id=lead["id"], name=lead.get("name", ""))

    def create_lead(
        self,
        contact_id: int,
        pipeline_id: int,
        status_id: int,
        name: str = "Instantly reply",
        campaign_name: str = "",
    ) -> KommoLead:
        """Create a new lead linked to a contact."""
        payload = [
            {
                "name": name,
                "pipeline_id": pipeline_id,
                "status_id": status_id,
                "_embedded": {
                    "contacts": [{"id": contact_id}],
                    "tags": [
                        {"name": "Instantly"},
                    ],
                },
            }
        ]

        if campaign_name:
            payload[0]["_embedded"]["tags"].append({"name": campaign_name})

        data = self._request("POST", "/leads", json=payload)
        created = data["_embedded"]["leads"][0]
        logger.info("Created Kommo lead id=%d for contact=%d", created["id"], contact_id)
        return KommoLead(id=created["id"], name=name)

    # --- Notes ---

    def add_note_to_lead(self, lead_id: int, text: str) -> int:
        """Add a text note to a lead. Returns note id."""
        payload = [
            {
                "note_type": "common",
                "params": {"text": text},
            }
        ]

        data = self._request("POST", f"/leads/{lead_id}/notes", json=payload)
        note_id = data["_embedded"]["notes"][0]["id"]
        logger.info("Added note id=%d to lead=%d", note_id, lead_id)
        return note_id


class KommoRateLimitError(Exception):
    """Raised when Kommo API returns 429."""

    def __init__(self, retry_after: int = 1) -> None:
        self.retry_after = retry_after
        super().__init__(f"Rate limited, retry after {retry_after}s")
