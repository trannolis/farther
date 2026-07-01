"""Sink contracts for Delta, Aurora, and quarantine writes."""

from __future__ import annotations

from typing import Any, Protocol

from src.validate import ValidationResult


class DeltaSink(Protocol):
    """Writes Bronze, Silver, Gold, and quarantine Delta tables."""

    def append_bronze(self, raw_payload: dict[str, Any], metadata: dict[str, Any]) -> None:
        ...

    def merge_silver(self, result: ValidationResult, metadata: dict[str, Any]) -> None:
        ...

    def write_quarantine(self, result: ValidationResult, metadata: dict[str, Any]) -> None:
        ...


class AuroraSink(Protocol):
    """Product-serving relational writes."""

    def upsert_client_snapshot(self, result: ValidationResult, metadata: dict[str, Any]) -> None:
        ...


class NoopDeltaSink:
    """Demo sink that makes orchestration testable without cloud credentials."""

    def append_bronze(self, raw_payload: dict[str, Any], metadata: dict[str, Any]) -> None:
        return None

    def merge_silver(self, result: ValidationResult, metadata: dict[str, Any]) -> None:
        return None

    def write_quarantine(self, result: ValidationResult, metadata: dict[str, Any]) -> None:
        return None


class NoopAuroraSink:
    """Demo sink that stands in for Aurora JDBC upserts."""

    def upsert_client_snapshot(self, result: ValidationResult, metadata: dict[str, Any]) -> None:
        return None
