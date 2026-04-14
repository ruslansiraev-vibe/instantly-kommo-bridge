"""Instantly API client for campaign metadata."""

from typing import Any

import httpx


class InstantlyClient:
    """Thin wrapper around Instantly API v2."""

    def __init__(self, api_key: str) -> None:
        self._base_url = "https://api.instantly.ai/api/v2"
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def list_campaigns(self) -> list[dict[str, Any]]:
        """
        Return a normalized campaign list.

        Response schema may differ by account/version, so this method accepts
        common containers: array root, {items: [...]}, or {data: [...]}.
        """
        with httpx.Client(timeout=15.0) as client:
            response = client.get(
                f"{self._base_url}/campaigns",
                headers=self._headers,
            )
        response.raise_for_status()
        payload = response.json()

        if isinstance(payload, list):
            raw_items = payload
        elif isinstance(payload, dict):
            raw_items = payload.get("items") or payload.get("data") or []
        else:
            raw_items = []

        normalized: list[dict[str, Any]] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            normalized.append(
                {
                    "id": item.get("id") or item.get("campaign_id") or "",
                    "name": item.get("name") or item.get("campaign_name") or "",
                    "status": item.get("status") or "",
                }
            )

        normalized.sort(key=lambda x: str(x["name"]).lower())
        return normalized
