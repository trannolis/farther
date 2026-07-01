"""Partner API contracts for the scheduled batch path."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol


@dataclass(frozen=True)
class DatasetRequest:
    request_id: str
    download_url: str


class PartnerDatasetClient(Protocol):
    """Adapter boundary for custodian APIs."""

    def request_dataset(self, as_of_date: date) -> str:
        """POST the daily request and return the partner request ID."""
        ...

    def poll_until_ready(self, request_id: str) -> DatasetRequest:
        """Poll partner status with retry/backoff until the dataset is ready."""
        ...

    def download_zip(self, dataset: DatasetRequest) -> bytes:
        """Stream the ready ZIP payload into memory or a temp file."""
        ...
