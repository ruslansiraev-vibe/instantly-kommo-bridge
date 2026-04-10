"""SQLite-based deduplication store for processed replies."""

import sqlite3
from datetime import datetime, timezone


class DedupStore:
    """Tracks processed Instantly emails to prevent duplicates in Kommo."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path)

    def _init_db(self) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_replies (
                    email_id TEXT PRIMARY KEY,
                    lead_email TEXT NOT NULL,
                    kommo_contact_id INTEGER,
                    kommo_lead_id INTEGER,
                    processed_at TEXT NOT NULL
                )
                """
            )

    def is_processed(self, email_id: str) -> bool:
        """Check if an email has already been processed."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM processed_replies WHERE email_id = ?",
                (email_id,),
            ).fetchone()
        return row is not None

    def mark_processed(
        self,
        email_id: str,
        lead_email: str,
        kommo_contact_id: int,
        kommo_lead_id: int,
    ) -> None:
        """Record a successfully processed reply."""
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO processed_replies
                    (email_id, lead_email, kommo_contact_id, kommo_lead_id, processed_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    email_id,
                    lead_email,
                    kommo_contact_id,
                    kommo_lead_id,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
