"""Skeleton for the 1k/10k scheduled Databricks Workflow path."""

from __future__ import annotations

import hashlib
import json
import zipfile
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Iterable

from src.ingest.config import PipelineSettings
from src.ingest.partner_client import PartnerDatasetClient
from src.ingest.reconcile import Reconciler, RunMetrics, metrics_from_results
from src.ingest.sinks import AuroraSink, DeltaSink
from src.ingest.storage import ObjectStore
from src.validate import ValidationResult, validate_client_file


@dataclass(frozen=True)
class BatchRunResult:
    request_id: str
    raw_zip_key: str
    metrics: RunMetrics


def iter_client_json_from_zip(zip_body: bytes) -> Iterable[dict[str, Any]]:
    """Yield JSON client payloads from a partner ZIP file."""
    with zipfile.ZipFile(BytesIO(zip_body)) as archive:
        for name in archive.namelist():
            if name.endswith("/"):
                continue
            if not name.lower().endswith(".json"):
                continue
            yield json.loads(archive.read(name))


def process_client_payload(
    raw_client: dict[str, Any],
    *,
    settings: PipelineSettings,
    delta_sink: DeltaSink,
    aurora_sink: AuroraSink,
    metadata: dict[str, Any],
) -> ValidationResult:
    """Validate one client and fan out valid/quarantined records to sinks."""
    result = validate_client_file(raw_client)

    delta_sink.append_bronze(raw_client, metadata)
    delta_sink.merge_silver(result, metadata)
    if result.has_quarantined_items:
        delta_sink.write_quarantine(result, metadata)
    if result.is_valid:
        aurora_sink.upsert_client_snapshot(result, metadata)

    return result


def run_daily_batch(
    *,
    settings: PipelineSettings,
    partner_client: PartnerDatasetClient,
    object_store: ObjectStore,
    delta_sink: DeltaSink,
    aurora_sink: AuroraSink,
    reconciler: Reconciler,
) -> BatchRunResult:
    """
    Orchestrate the scheduled batch path.

    In production, Databricks Workflows would split this into tasks:
    auth_notebook, request_notebook, poll_notebook, download_notebook,
    extract_notebook, process_notebook, and reconcile_notebook.
    """
    request_id = partner_client.request_dataset(settings.as_of_date)
    dataset = partner_client.poll_until_ready(request_id)
    zip_body = partner_client.download_zip(dataset)

    raw_zip_key = settings.zip_key(request_id)
    object_store.put_bytes(raw_zip_key, zip_body)

    results: list[ValidationResult] = []
    for raw_client in iter_client_json_from_zip(zip_body):
        client_id = str(raw_client.get("id", "UNKNOWN"))
        source_hash = hashlib.sha256(
            json.dumps(raw_client, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]
        client_key = settings.client_key(client_id, source_hash)
        object_store.put_json(client_key, raw_client)

        metadata = {
            "batch_id": settings.batch_id,
            "as_of_date": settings.as_of_date.isoformat(),
            "partner_name": settings.partner_name,
            "raw_client_key": client_key,
            "raw_zip_key": raw_zip_key,
        }
        results.append(
            process_client_payload(
                raw_client,
                settings=settings,
                delta_sink=delta_sink,
                aurora_sink=aurora_sink,
                metadata=metadata,
            )
        )

    metrics = metrics_from_results(results)
    reconciler.emit_metrics(metrics)

    return BatchRunResult(
        request_id=request_id,
        raw_zip_key=raw_zip_key,
        metrics=metrics,
    )
