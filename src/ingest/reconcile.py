"""Reconciliation metrics emitted after batch or streaming micro-batches."""

from __future__ import annotations

from dataclasses import dataclass

from src.validate import ValidationResult


@dataclass(frozen=True)
class RunMetrics:
    client_payloads_seen: int
    valid_accounts: int
    valid_holdings: int
    valid_transactions: int
    quarantined_records: int

    @property
    def quarantine_rate(self) -> float:
        total_records = (
            self.valid_accounts
            + self.valid_holdings
            + self.valid_transactions
            + self.quarantined_records
        )
        if total_records == 0:
            return 0.0
        return self.quarantined_records / total_records


def metrics_from_results(results: list[ValidationResult]) -> RunMetrics:
    return RunMetrics(
        client_payloads_seen=len(results),
        valid_accounts=sum(len(result.valid_accounts) for result in results),
        valid_holdings=sum(len(result.valid_holdings) for result in results),
        valid_transactions=sum(len(result.valid_transactions) for result in results),
        quarantined_records=sum(
            len(result.quarantined_accounts)
            + len(result.quarantined_holdings)
            + len(result.quarantined_transactions)
            for result in results
        ),
    )


class Reconciler:
    """Placeholder for file counts, control totals, and CloudWatch metrics."""

    def emit_metrics(self, metrics: RunMetrics) -> None:
        return None
