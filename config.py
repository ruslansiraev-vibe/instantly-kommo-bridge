"""Configuration loaded from environment variables."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """Immutable application configuration."""

    instantly_api_key: str
    instantly_webhook_secret: str
    kommo_subdomain: str
    kommo_token: str
    kommo_pipeline_id: int
    kommo_pipeline_status_id: int
    db_path: str
    log_level: str

    @staticmethod
    def from_env() -> "Config":
        """Load config from environment variables. Fail fast if required vars missing."""
        required = [
            "INSTANTLY_API_KEY",
            "INSTANTLY_WEBHOOK_SECRET",
            "KOMMO_SUBDOMAIN",
            "KOMMO_TOKEN",
            "KOMMO_PIPELINE_ID",
            "KOMMO_PIPELINE_STATUS_ID",
        ]
        missing = [var for var in required if not os.getenv(var)]
        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        return Config(
            instantly_api_key=os.environ["INSTANTLY_API_KEY"],
            instantly_webhook_secret=os.environ["INSTANTLY_WEBHOOK_SECRET"],
            kommo_subdomain=os.environ["KOMMO_SUBDOMAIN"],
            kommo_token=os.environ["KOMMO_TOKEN"],
            kommo_pipeline_id=int(os.environ["KOMMO_PIPELINE_ID"]),
            kommo_pipeline_status_id=int(os.environ["KOMMO_PIPELINE_STATUS_ID"]),
            db_path=os.getenv("DB_PATH", "processed_replies.db"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
