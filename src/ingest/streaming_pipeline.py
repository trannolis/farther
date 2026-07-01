"""Skeleton for the 1M-client event-driven ingestion path."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.ingest.config import PipelineSettings
from src.ingest.sinks import AuroraSink, DeltaSink
from src.ingest.storage import ObjectStore
from src.validate import ValidationResult, validate_client_file


@dataclass(frozen=True)
class PartnerEvent:
    event_id: str
    client_id: str
    payload: dict[str, Any]


def land_partner_event(
    *,
    event: PartnerEvent,
    settings: PipelineSettings,
    object_store: ObjectStore,
) -> str:
    """
    Landing Lambda behavior: verify at the edge, then store raw JSON durably.

    HMAC/JWT verification belongs in API Gateway or a thin authorizer before
    this function is called.
    """
    key = settings.event_key(event.event_id)
    object_store.put_json(
        key,
        {
            "event_id": event.event_id,
            "client_id": event.client_id,
            "partner_name": settings.partner_name,
            "payload": event.payload,
        },
    )
    return key


def process_landed_event(
    *,
    raw_event: dict[str, Any],
    settings: PipelineSettings,
    delta_sink: DeltaSink,
    aurora_sink: AuroraSink,
) -> ValidationResult:
    """
    Structured Streaming micro-batch behavior for one landed event.

    Databricks Auto Loader owns file discovery and checkpointing. Delta MERGE
    should use event_id as the dedupe key for Silver writes.
    """
    event_id = str(raw_event["event_id"])
    payload = raw_event["payload"]
    result = validate_client_file(payload)

    metadata = {
        "event_id": event_id,
        "batch_id": settings.batch_id,
        "partner_name": settings.partner_name,
        "as_of_date": settings.as_of_date.isoformat(),
    }

    delta_sink.append_bronze(raw_event, metadata)
    delta_sink.merge_silver(result, metadata)
    if result.has_quarantined_items:
        delta_sink.write_quarantine(result, metadata)
    if result.is_valid:
        aurora_sink.upsert_client_snapshot(result, metadata)

    return result
