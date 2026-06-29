# Custodial Data Validation Use Case

This sequence flow shows how a raw custodian payload moves through the boundary
validation layer before any database write. The goal is to accept clean records,
quarantine bad sub-entities, and avoid rejecting an entire client because one
holding or transaction is malformed.

```mermaid
sequenceDiagram
    autonumber
    actor Custodian as Custodian Feed
    participant Parser as JSON Parser
    participant Entry as validate_client_file()
    participant Account as AccountValidator
    participant Holding as HoldingValidator
    participant Txn as TransactionValidator
    participant Result as ValidationResult
    participant Store as Downstream Store
    participant Ops as Data Quality Review

    Custodian->>Parser: Send client JSON payload
    Parser->>Entry: Parsed raw_json
    Entry->>Result: Initialize result with client id/name

    loop accounts[]
        Entry->>Account: model_validate(raw account)
        alt account passes field validation
            Account-->>Result: Append valid account
        else invalid value/currency
            Account-->>Result: Quarantine account with validation errors
        end
    end

    loop holdings[]
        Entry->>Holding: model_validate(raw holding)
        alt holding passes field validation
            Holding-->>Result: Append valid holding
        else missing non-cash security or invalid quantity/buyPrice
            Holding-->>Result: Quarantine holding with validation errors
        end
    end

    loop transactions[]
        Entry->>Txn: model_validate(raw transaction)
        alt transaction passes field validation
            Txn-->>Result: Append valid transaction
        else invalid type, quantity, or settle date
            Txn-->>Result: Quarantine transaction with validation errors
        end
    end

    Entry->>Entry: Build valid account and holding id sets

    loop valid holdings
        Entry->>Result: Check accountId exists in valid accounts
        alt accountId exists
            Result-->>Entry: Keep holding valid
        else accountId missing/quarantined
            Entry->>Result: Move holding to quarantine
        end
    end

    loop valid transactions
        Entry->>Result: Check accountId and holdingId references
        alt references exist
            Result-->>Entry: Keep transaction valid
        else accountId or holdingId missing/quarantined
            Entry->>Result: Move transaction to quarantine
        end
    end

    Entry-->>Store: Persist valid accounts, holdings, transactions
    Entry-->>Ops: Send quarantined records and errors for review
```

## Key Decisions

- Validation happens at the ingestion boundary, before database writes.
- Each sub-entity is validated independently, so one bad holding does not poison
  the rest of the client payload.
- Referential-integrity checks run only after field/model validation, using the
  surviving valid records as the trusted id set.
- Quarantined records retain the raw payload, entity type, error details, and
  quarantine reason so data quality issues can be reviewed and remediated.
