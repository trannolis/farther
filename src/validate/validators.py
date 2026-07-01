"""
Farther — Custodial Data Validation Layer
Boundary validators using Pydantic v2.
Run at parse time, before any DB write.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Optional
from enum import Enum

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUPPORTED_CURRENCIES = {"USD", "CAD", "GBP", "EUR", "JPY", "CHF", "AUD"}
VALID_TRANSACTION_TYPES = {"BUY", "SELL", "DIV", "INT", "FEE", "TRANSFER", "SPLIT"}
MAX_SETTLE_DAYS = 10  # T+10 is unusual; flag anything longer


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------

class AccountValidator(BaseModel):
    id: str
    value: Any  # Raw: sometimes a string ("1208"), sometimes a number
    currency: str
    name: str
    type: str

    @field_validator("value", mode="before")
    @classmethod
    def coerce_value_to_decimal(cls, v):
        """Partner sends value as a string. Convert to float."""
        try:
            result = float(v)
        except (TypeError, ValueError):
            raise ValueError(f"account.value must be numeric, got {v!r}")
        if result < 0:
            raise ValueError(f"account.value cannot be negative, got {result}")
        return result

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v):
        upper = v.upper() if v else ""
        if upper not in SUPPORTED_CURRENCIES:
            raise ValueError(
                f"Unsupported currency {v!r}. Expected one of {SUPPORTED_CURRENCIES}"
            )
        return upper


class HoldingValidator(BaseModel):
    id: str
    accountId: str
    name: str
    security: Optional[str] = None
    quantity: float
    buyPrice: float
    isCashLike: bool = False

    @field_validator("security", mode="before")
    @classmethod
    def normalize_security(cls, v):
        # Treat empty string same as None
        return v.strip() if v and v.strip() else None

    @model_validator(mode="after")
    def security_required_for_non_cash(self) -> "HoldingValidator":
        """
        Non-cash holdings MUST have a security identifier.
        A bond like 'Apple Bond 2026 6%' has security=None — this is a known
        data quality issue with fixed income from some custodians. We quarantine
        these rather than reject the whole client.
        """
        if not self.isCashLike and self.security is None:
            raise ValueError(
                f"Non-cash holding '{self.name}' (id={self.id}) has no security "
                f"identifier. For fixed income, expect ISIN or CUSIP instead of ticker."
            )
        return self

    @field_validator("quantity")
    @classmethod
    def quantity_must_be_positive(cls, v):
        if v < 0:
            raise ValueError(f"Quantity must be >= 0, got {v}")
        return v

    @field_validator("buyPrice")
    @classmethod
    def buy_price_must_be_non_negative(cls, v):
        if v < 0:
            raise ValueError(f"buyPrice cannot be negative, got {v}")
        return v


class TransactionValidator(BaseModel):
    id: str
    accountId: str
    holdingId: Optional[str] = None
    type: str
    quantity: Optional[float] = None
    value: Optional[float] = None
    date: date
    settleDate: Optional[date] = None

    @field_validator("type")
    @classmethod
    def validate_transaction_type(cls, v):
        upper = v.upper() if v else ""
        if upper not in VALID_TRANSACTION_TYPES:
            raise ValueError(
                f"Unknown transaction type {v!r}. Expected one of {VALID_TRANSACTION_TYPES}"
            )
        return upper

    @field_validator("quantity")
    @classmethod
    def validate_quantity(cls, v):
        if v is not None and v < 0:
            raise ValueError(f"Transaction quantity cannot be negative, got {v}")
        return v

    @model_validator(mode="after")
    def validate_settle_date(self) -> "TransactionValidator":
        if self.settleDate and self.settleDate < self.date:
            raise ValueError(
                f"settleDate {self.settleDate} is before tradeDate {self.date}"
            )
        if self.settleDate:
            delta = (self.settleDate - self.date).days
            if delta > MAX_SETTLE_DAYS:
                raise ValueError(
                    f"Settle lag of {delta} days is unusually long "
                    f"(tradeDate={self.date}, settleDate={self.settleDate})"
                )
        return self


class ClientValidator(BaseModel):
    id: str
    name: str
    accounts: list[AccountValidator] = Field(default_factory=list)
    holdings: list[HoldingValidator] = Field(default_factory=list)
    transactions: list[TransactionValidator] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_referential_integrity(self) -> "ClientValidator":
        """
        Holdings must reference valid account IDs.
        Transactions must reference valid account IDs (and optionally holding IDs).
        """
        account_ids = {a.id for a in self.accounts}
        holding_ids = {h.id for h in self.holdings}

        for h in self.holdings:
            if h.accountId not in account_ids:
                raise ValueError(
                    f"Holding {h.id} references accountId {h.accountId!r} "
                    f"which does not exist in client {self.id}"
                )

        for t in self.transactions:
            if t.accountId not in account_ids:
                raise ValueError(
                    f"Transaction {t.id} references accountId {t.accountId!r} "
                    f"which does not exist in client {self.id}"
                )
            if t.holdingId and t.holdingId not in holding_ids:
                raise ValueError(
                    f"Transaction {t.id} references holdingId {t.holdingId!r} "
                    f"which does not exist in client {self.id}"
                )

        return self


# ---------------------------------------------------------------------------
# Per-field quarantine: validate each sub-entity independently
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    client_id: str
    client_name: str = ""

    # Valid sub-entities (passed all validators)
    valid_accounts: list[dict] = field(default_factory=list)
    valid_holdings: list[dict] = field(default_factory=list)
    valid_transactions: list[dict] = field(default_factory=list)

    # Quarantined sub-entities (failed validation)
    quarantined_accounts: list[dict] = field(default_factory=list)
    quarantined_holdings: list[dict] = field(default_factory=list)
    quarantined_transactions: list[dict] = field(default_factory=list)

    # Top-level client errors (client rejected entirely)
    client_errors: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not self.client_errors

    @property
    def has_quarantined_items(self) -> bool:
        return bool(
            self.quarantined_accounts
            or self.quarantined_holdings
            or self.quarantined_transactions
        )

    def summary(self) -> dict:
        return {
            "client_id": self.client_id,
            "is_valid": self.is_valid,
            "accounts": {
                "valid": len(self.valid_accounts),
                "quarantined": len(self.quarantined_accounts),
            },
            "holdings": {
                "valid": len(self.valid_holdings),
                "quarantined": len(self.quarantined_holdings),
            },
            "transactions": {
                "valid": len(self.valid_transactions),
                "quarantined": len(self.quarantined_transactions),
            },
            "client_errors": self.client_errors,
        }


def _validate_list(
    raw_items: list[dict],
    validator_class: type[BaseModel],
    valid_out: list,
    quarantined_out: list,
    entity_label: str,
) -> None:
    """
    Validate each item in raw_items individually.
    Valid items go to valid_out; failed items go to quarantined_out.
    One bad item never poisons the others.
    """
    from pydantic import ValidationError

    for item in raw_items:
        try:
            validated = validator_class.model_validate(item)
            valid_out.append(validated.model_dump())
        except ValidationError as exc:
            quarantined_out.append(
                {
                    "raw": item,
                    "entity_type": entity_label,
                    "errors": exc.errors(include_url=False),
                    "quarantine_reason": "validation_failure",
                }
            )


def validate_client_file(raw_json: dict) -> ValidationResult:
    """
    Top-level entry point. Validates a single client's JSON payload.

    Strategy:
    1. Validate accounts, holdings, transactions independently.
       Each bad sub-entity is quarantined; the rest proceed.
    2. After per-entity validation, run cross-entity referential integrity
       on the surviving valid items only.

    Returns a ValidationResult with separate valid/quarantined lists per entity type.
    """
    from pydantic import ValidationError

    client_id = raw_json.get("id", "UNKNOWN")
    result = ValidationResult(
        client_id=client_id,
        client_name=raw_json.get("name", ""),
    )

    # Step 1: validate sub-entities independently
    _validate_list(
        raw_json.get("accounts", []),
        AccountValidator,
        result.valid_accounts,
        result.quarantined_accounts,
        "account",
    )
    _validate_list(
        raw_json.get("holdings", []),
        HoldingValidator,
        result.valid_holdings,
        result.quarantined_holdings,
        "holding",
    )
    _validate_list(
        raw_json.get("transactions", []),
        TransactionValidator,
        result.valid_transactions,
        result.quarantined_transactions,
        "transaction",
    )

    # Step 2: cross-entity referential integrity on valid items
    valid_account_ids = {a["id"] for a in result.valid_accounts}
    valid_holding_ids = {h["id"] for h in result.valid_holdings}

    # Re-quarantine holdings that reference a quarantined/missing account
    still_valid_holdings = []
    for h in result.valid_holdings:
        if h["accountId"] not in valid_account_ids:
            result.quarantined_holdings.append(
                {
                    "raw": h,
                    "entity_type": "holding",
                    "errors": [
                        {
                            "type": "referential_integrity",
                            "msg": f"accountId {h['accountId']!r} not in valid accounts",
                        }
                    ],
                    "quarantine_reason": "referential_integrity_failure",
                }
            )
        else:
            still_valid_holdings.append(h)
    result.valid_holdings = still_valid_holdings

    # Re-quarantine transactions that reference missing accounts or holdings
    still_valid_txns = []
    for t in result.valid_transactions:
        errors = []
        if t["accountId"] not in valid_account_ids:
            errors.append({"type": "referential_integrity", "msg": f"accountId {t['accountId']!r} missing"})
        if t.get("holdingId") and t["holdingId"] not in valid_holding_ids:
            errors.append({"type": "referential_integrity", "msg": f"holdingId {t['holdingId']!r} missing"})
        if errors:
            result.quarantined_transactions.append(
                {
                    "raw": t,
                    "entity_type": "transaction",
                    "errors": errors,
                    "quarantine_reason": "referential_integrity_failure",
                }
            )
        else:
            still_valid_txns.append(t)
    result.valid_transactions = still_valid_txns

    return result


# ---------------------------------------------------------------------------
# Demo — run against the sample JSON from the brief
# ---------------------------------------------------------------------------

SAMPLE_CLIENT = {
    "id": "c_1234",
    "name": "John Adams",
    "accounts": [
        {"id": "a_1234", "value": "1208", "currency": "USD", "name": "Brokerage", "type": "Brokerage"},
        {"id": "a_2345", "value": "1045", "currency": "CAD", "name": "John's Retirement", "type": "IRA"},
    ],
    "holdings": [
        {
            "id": "h_1234", "accountId": "a_1234",
            "name": "Apple Inc", "security": "AAPL",
            "quantity": 14.5, "buyPrice": 145, "isCashLike": False,
        },
        {
            # This one will be QUARANTINED: non-cash, security=null
            "id": "h_2345", "accountId": "a_1234",
            "name": "Apple Bond 2026 6%", "security": None,
            "quantity": 140, "buyPrice": 0.98, "isCashLike": False,
        },
    ],
    "transactions": [
        {
            "id": "t_1234", "accountId": "a_1234", "holdingId": "h_1234",
            "type": "SELL", "quantity": 2, "value": 167,
            "date": "2024-04-13", "settleDate": "2024-04-15",
        }
    ],
}


if __name__ == "__main__":
    result = validate_client_file(SAMPLE_CLIENT)
    print(json.dumps(result.summary(), indent=2))
    print(f"\nValid holdings:      {len(result.valid_holdings)}")
    print(f"Quarantined holdings:{len(result.quarantined_holdings)}")
    for q in result.quarantined_holdings:
        print(f"  → {q['raw']['name']}: {q['errors']}")
