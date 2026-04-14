"""SQLite store for routing Instantly campaigns to Kommo pipelines."""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass(frozen=True)
class CampaignRoute:
    campaign_name: str
    pipeline_id: int
    status_id: int
    updated_at: str


class CampaignRoutingStore:
    """Stores campaign->pipeline/status overrides."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path)

    def _init_db(self) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS campaign_routes (
                    campaign_name TEXT PRIMARY KEY,
                    pipeline_id INTEGER NOT NULL,
                    status_id INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

    def list_routes(self) -> list[CampaignRoute]:
        with self._get_conn() as conn:
            rows = conn.execute(
                """
                SELECT campaign_name, pipeline_id, status_id, updated_at
                FROM campaign_routes
                ORDER BY campaign_name COLLATE NOCASE ASC
                """
            ).fetchall()

        return [
            CampaignRoute(
                campaign_name=row[0],
                pipeline_id=row[1],
                status_id=row[2],
                updated_at=row[3],
            )
            for row in rows
        ]

    def get_route(self, campaign_name: str) -> Optional[CampaignRoute]:
        with self._get_conn() as conn:
            row = conn.execute(
                """
                SELECT campaign_name, pipeline_id, status_id, updated_at
                FROM campaign_routes
                WHERE campaign_name = ?
                """,
                (campaign_name,),
            ).fetchone()

        if row is None:
            return None

        return CampaignRoute(
            campaign_name=row[0],
            pipeline_id=row[1],
            status_id=row[2],
            updated_at=row[3],
        )

    def upsert_route(self, campaign_name: str, pipeline_id: int, status_id: int) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO campaign_routes (campaign_name, pipeline_id, status_id, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(campaign_name) DO UPDATE SET
                    pipeline_id = excluded.pipeline_id,
                    status_id = excluded.status_id,
                    updated_at = excluded.updated_at
                """,
                (
                    campaign_name,
                    pipeline_id,
                    status_id,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def delete_route(self, campaign_name: str) -> None:
        with self._get_conn() as conn:
            conn.execute(
                "DELETE FROM campaign_routes WHERE campaign_name = ?",
                (campaign_name,),
            )
