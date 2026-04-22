"""Kommo CRM API v4 client for contacts, leads (deals), and notes.

Bugfix (Apr 2026): 
- Added detailed logging for all API calls and errors
- Improved find_active_lead_by_contact (accepts pipeline_id, more robust status check, increased limit)
- Better error messages when pipeline_id or status_id is invalid
"""

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
        logger.debug("Kommo %s %s %s", method, url, kwargs.get("params") or kwargs.get("json", "")[:100])

        with httpx.Client(timeout=KOMMO_API_TIMEOUT) as client:
            response = client.request(
                method, url, headers=self._headers, **kwargs
            )

        if response.status_code in (204, 404):
            logger.debug("Kommo %s %s returned %s", method, path, response.status_code)
            return None

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "1")
            logger.warning("Kommo rate limit hit, retry after %s sec", retry_after)
            raise KommoRateLimitError(retry_after=int(retry_after))

        if response.status_code >= 400:
            try:
                error_detail = response.json()
            except Exception:
                error_detail = response.text
            logger.error(
                "Kommo API error: %s %s -> %s %s",
                method,
                path,
                response.status_code,
                error_detail,
            )

        response.raise_for_status()
        data = response.json()
        logger.debug("Kommo %s %s success: %s", method, path, list(data.keys()) if isinstance(data, dict) else "ok")
        return data

    def list_pipelines(self) -> list[dict]:
        """Return pipelines with statuses for admin routing UI."""
        data = self._request("GET", "/leads/pipelines", params={"limit": 250})
        if not data:
            return []

        pipelines = data.get("_embedded", {}).get("pipelines", [])
        result: list[dict] = []
        for p in pipelines:
            statuses = []
            for s in p.get("_embedded", {}).get("statuses", []):
                statuses.append(
                    {
                        "id": s.get("id"),
                        "name": s.get("name", ""),
                    }
                )
            result.append(
                {
                    "id": p.get("id"),
                    "name": p.get("name", ""),
                    "statuses": statuses,
                }
            )
        return result

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

        # COMPANY is not a standard field_code in all Kommo accounts;
        # store it as a tag on the contact instead to avoid 400 errors.
        if company:
            payload[0]["_embedded"]["tags"].append({"name": company})

        data = self._request("POST", "/contacts", json=payload)
        created = data["_embedded"]["contacts"][0]
        logger.info("Created Kommo contact id=%d for %s", created["id"], email)
        return KommoContact(id=created["id"], name=name)

    # --- Leads (deals) ---

    def find_active_lead_by_contact(
        self, contact_id: int, pipeline_id: Optional[int] = None
    ) -> Optional[KommoLead]:
        """Find an active (open) lead linked to a contact in the given pipeline (if provided).

        Uses the /contacts/{id}/links endpoint to get leads actually linked
        to this contact, then checks their status via /leads.
        """
        links_data = self._request(
            "GET",
            f"/contacts/{contact_id}/links",
            params={"limit": 50},
        )
        if not links_data:
            logger.debug("No links found for contact_id=%s", contact_id)
            return None

        linked_lead_ids = [
            link["to_entity_id"]
            for link in links_data.get("_embedded", {}).get("links", [])
            if link.get("to_entity_type") == "leads"
        ]
        if not linked_lead_ids:
            logger.debug("Contact %s has no linked leads", contact_id)
            return None

        params: dict = {
            "filter[id]": linked_lead_ids,
            "order[updated_at]": "desc",
        }
        if pipeline_id:
            params["filter[pipeline_id]"] = pipeline_id

        data = self._request("GET", "/leads", params=params)
        if not data:
            return None

        leads = data.get("_embedded", {}).get("leads", [])
        if not leads:
            logger.debug("No matching leads for contact_id=%s (linked_ids=%s)", contact_id, linked_lead_ids)
            return None

        for lead in leads:
            status_id = lead.get("status_id")
            if status_id not in (142, 143):
                logger.debug("Found active lead id=%s status_id=%s for contact=%s", lead["id"], status_id, contact_id)
                return KommoLead(id=lead["id"], name=lead.get("name", ""))

        logger.debug("All linked leads for contact=%s are closed", contact_id)
        return None

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

        logger.info(
            "Creating lead: name='%s', pipeline_id=%s, status_id=%s, contact_id=%s",
            name, pipeline_id, status_id, contact_id
        )

        data = self._request("POST", "/leads", json=payload)
        created = data["_embedded"]["leads"][0]
        logger.info("Created Kommo lead id=%d for contact=%d (pipeline=%d, status=%d)", 
                   created["id"], contact_id, pipeline_id, status_id)
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


    # --- Tasks ---

    def create_task(
        self,
        lead_id: int,
        responsible_user_id: int,
        text: str,
        complete_till_seconds: int = 86400,
    ) -> int:
        """Create a task linked to a lead. Returns task id.

        Args:
            complete_till_seconds: deadline offset from now (default 24h).
        """
        import time

        payload = [
            {
                "task_type_id": 1,
                "text": text,
                "complete_till": int(time.time()) + complete_till_seconds,
                "entity_id": lead_id,
                "entity_type": "leads",
                "responsible_user_id": responsible_user_id,
            }
        ]

        data = self._request("POST", "/tasks", json=payload)
        task_id = data["_embedded"]["tasks"][0]["id"]
        logger.info(
            "Created task id=%d on lead=%d for user=%d",
            task_id, lead_id, responsible_user_id,
        )
        return task_id

    # --- Users ---

    def list_users(self) -> list[dict]:
        """Return list of account users [{id, name, email}]."""
        data = self._request("GET", "/users", params={"limit": 250})
        if not data:
            return []

        users = data.get("_embedded", {}).get("users", [])
        return [
            {
                "id": u.get("id"),
                "name": u.get("name", ""),
                "email": u.get("email", ""),
            }
            for u in users
        ]


class KommoRateLimitError(Exception):
    """Raised when Kommo API returns 429."""

    def __init__(self, retry_after: int = 1) -> None:
        self.retry_after = retry_after
        super().__init__(f"Rate limited, retry after {retry_after}s")
