"""Configuration and stable object-key naming for ingestion runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class PipelineSettings:
    """Minimal settings shared by batch and event-driven ingestion paths."""

    partner_name: str
    bucket_name: str
    as_of_date: date
    batch_id: str

    raw_zip_prefix: str = "raw/zips"
    raw_client_prefix: str = "raw/clients"
    raw_event_prefix: str = "raw/events"

    def zip_key(self, request_id: str) -> str:
        return (
            f"{self.raw_zip_prefix}/partner={self.partner_name}/"
            f"as_of_date={self.as_of_date.isoformat()}/request_id={request_id}.zip"
        )

    def client_key(self, client_id: str, source_file_hash: str) -> str:
        return (
            f"{self.raw_client_prefix}/partner={self.partner_name}/"
            f"as_of_date={self.as_of_date.isoformat()}/client_id={client_id}/"
            f"{source_file_hash}.json"
        )

    def event_key(self, event_id: str) -> str:
        return (
            f"{self.raw_event_prefix}/partner={self.partner_name}/"
            f"event_id={event_id}/payload.json"
        )
