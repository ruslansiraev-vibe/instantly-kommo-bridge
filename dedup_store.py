"""SQLite-based deduplication store for processed replies."""

import sqlite3
from datetime import datetime, timezone
from typing import Optional


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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS webhook_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    received_at TEXT NOT NULL,
                    event_type TEXT NOT NULL DEFAULT '',
                    instantly_status TEXT NOT NULL DEFAULT '',
                    lead_email TEXT NOT NULL DEFAULT '',
                    campaign_name TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'received',
                    reply_snippet TEXT NOT NULL DEFAULT '',
                    kommo_contact_id INTEGER,
                    kommo_lead_id INTEGER,
                    error_message TEXT NOT NULL DEFAULT ''
                )
                """
            )
            # Migration: add instantly_status column if missing
            try:
                conn.execute("SELECT instantly_status FROM webhook_log LIMIT 0")
            except sqlite3.OperationalError:
                conn.execute(
                    "ALTER TABLE webhook_log ADD COLUMN instantly_status TEXT NOT NULL DEFAULT ''"
                )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_wl_received_at ON webhook_log(received_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_wl_status ON webhook_log(status)"
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

    def try_claim(self, email_id: str, lead_email: str) -> bool:
        """
        Atomically claim a webhook event for processing.

        Returns True only for the first worker that inserts this email_id.
        Prevents race-condition duplicates when identical webhooks arrive in parallel.
        """
        with self._get_conn() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO processed_replies
                    (email_id, lead_email, processed_at)
                VALUES (?, ?, ?)
                """,
                (
                    email_id,
                    lead_email,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            return cur.rowcount == 1

    def complete_claim(
        self,
        email_id: str,
        kommo_contact_id: int,
        kommo_lead_id: int,
    ) -> None:
        """Attach Kommo entity IDs to an already claimed event."""
        with self._get_conn() as conn:
            conn.execute(
                """
                UPDATE processed_replies
                SET kommo_contact_id = ?, kommo_lead_id = ?
                WHERE email_id = ?
                """,
                (kommo_contact_id, kommo_lead_id, email_id),
            )

    def release_claim(self, email_id: str) -> None:
        """Release a claim so failed events can be retried safely."""
        with self._get_conn() as conn:
            conn.execute(
                "DELETE FROM processed_replies WHERE email_id = ?",
                (email_id,),
            )

    # --- Webhook log ---

    def log_webhook(
        self,
        event_type: str,
        lead_email: str,
        campaign_name: str,
        status: str,
        instantly_status: str = "",
        reply_snippet: str = "",
        kommo_contact_id: Optional[int] = None,
        kommo_lead_id: Optional[int] = None,
        error_message: str = "",
    ) -> int:
        """Record a webhook event to the log. Returns the log row id."""
        with self._get_conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO webhook_log
                    (received_at, event_type, instantly_status, lead_email, campaign_name,
                     status, reply_snippet, kommo_contact_id, kommo_lead_id, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    event_type,
                    instantly_status,
                    lead_email,
                    campaign_name,
                    status,
                    reply_snippet[:300],
                    kommo_contact_id,
                    kommo_lead_id,
                    error_message[:500],
                ),
            )
            return cur.lastrowid or 0

    def update_webhook_log(
        self,
        log_id: int,
        status: str,
        kommo_contact_id: Optional[int] = None,
        kommo_lead_id: Optional[int] = None,
        error_message: str = "",
    ) -> None:
        """Update an existing webhook log entry after processing."""
        with self._get_conn() as conn:
            conn.execute(
                """
                UPDATE webhook_log
                SET status = ?, kommo_contact_id = ?, kommo_lead_id = ?, error_message = ?
                WHERE id = ?
                """,
                (status, kommo_contact_id, kommo_lead_id, error_message[:500], log_id),
            )

    def get_webhook_logs(
        self,
        limit: int = 100,
        offset: int = 0,
        status_filter: Optional[str] = None,
        email_filter: Optional[str] = None,
        event_type_filter: Optional[str] = None,
        instantly_status_filter: Optional[str] = None,
    ) -> list[dict]:
        """Query webhook log entries with optional filters."""
        conditions = []
        params: list = []

        if status_filter:
            conditions.append("status = ?")
            params.append(status_filter)
        if email_filter:
            conditions.append("lead_email LIKE ?")
            params.append(f"%{email_filter}%")
        if event_type_filter:
            conditions.append("event_type = ?")
            params.append(event_type_filter)
        if instantly_status_filter:
            conditions.append("instantly_status = ?")
            params.append(instantly_status_filter)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        with self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                SELECT id, received_at, event_type, instantly_status, lead_email, campaign_name,
                       status, reply_snippet, kommo_contact_id, kommo_lead_id, error_message
                FROM webhook_log
                {where}
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """,
                (*params, limit, offset),
            ).fetchall()

            total = conn.execute(
                f"SELECT COUNT(*) FROM webhook_log {where}",
                params,
            ).fetchone()[0]

        return {"rows": [dict(r) for r in rows], "total": total}

    def get_webhook_log_stats(self) -> dict:
        """Get summary counts by status."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) as cnt FROM webhook_log GROUP BY status"
            ).fetchall()
        return {row[0]: row[1] for row in rows}
