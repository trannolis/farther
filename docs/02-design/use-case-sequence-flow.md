# Custodial Data Ingestion Use Case Flow

This flow is updated to match the three current architecture diagrams:

- `docs/diagrams/scale-1k.png`: daily Databricks Workflow batch pipeline.
- `docs/diagrams/scale-10k.png`: same batch pipeline with auto-scaling, RDS Proxy, and Delta `OPTIMIZE`.
- `docs/diagrams/scale-1m.png`: event-driven webhook landing plus Databricks Auto Loader and Structured Streaming.

The invariant across all three designs is the data contract: raw partner data is
stored durably before transformation, validation happens before Silver Delta or
Aurora writes, and bad sub-entities are quarantined instead of rejecting the
entire client payload.

## Scale 1k: Daily Batch Workflow

At 1,000 clients, the simple path is a single scheduled Databricks Workflow. The
main risk is not compute scale; it is losing replayability or writing untrusted
partner data into product tables.

```mermaid
sequenceDiagram
    autonumber
    participant Workflow as Databricks Workflow
    participant Secrets as Secret Scope
    participant Partner as Partner API
    participant S3Zip as S3 raw/zips
    participant S3Client as S3 raw/clients
    participant Spark as process_notebook
    participant Bronze as Delta Bronze
    participant Silver as Delta Silver
    participant Quarantine as Delta Quarantine
    participant Aurora as Aurora Postgres
    participant Ops as CloudWatch + Reconcile

    Workflow->>Secrets: Read OAuth client credentials
    Workflow->>Partner: auth_notebook requests OAuth token
    Workflow->>Partner: request_notebook POSTs dataset request
    Partner-->>Workflow: request_id
    loop retry with backoff
        Workflow->>Partner: poll_notebook checks request status
        Partner-->>Workflow: pending or ready
    end
    Workflow->>Partner: download_notebook streams ZIP
    Workflow->>S3Zip: Store raw ZIP boundary
    Workflow->>S3Zip: extract_notebook reads raw ZIP
    Workflow->>S3Client: Write raw client JSON boundary
    Workflow->>Spark: process_notebook reads client files in parallel
    Spark->>Bronze: Append raw events by as_of_date
    Spark->>Spark: Parse + validate accounts, holdings, transactions
    alt record is valid
        Spark->>Silver: MERGE normalized accounts, holdings, transactions
        Spark->>Aurora: JDBC upsert for product API queries
    else record fails field or reference validation
        Spark->>Quarantine: Write raw payload + errors
    end
    Spark->>Ops: reconcile_notebook emits counts, totals, alerts
```

### 1k Operating Notes

- Durable boundaries are `raw/zips/` and `raw/clients/`. A failed process step
  can replay from S3 without calling the partner again.
- Spark parallelizes per-client validation inside the processing task.
- Aurora writes are acceptable directly over JDBC at this scale.
- CloudWatch receives file counts, control totals, quarantine counts, and job
  duration from `reconcile_notebook`.

## Scale 10k: Auto-Scaled Batch

At 10,000 clients, the workflow shape stays the same. The changes are operational
knobs around throughput and write pressure.

```mermaid
sequenceDiagram
    autonumber
    participant Workflow as Databricks Workflow
    participant S3Client as S3 raw/clients
    participant Spark as Auto-Scale Spark Cluster
    participant Bronze as Delta Bronze
    participant Silver as Delta Silver
    participant Quarantine as Delta Quarantine
    participant Proxy as RDS Proxy
    participant Aurora as Aurora Postgres
    participant Ops as CloudWatch

    Workflow->>S3Client: Raw client JSON already landed
    Workflow->>Spark: process_notebook starts with min/max workers
    Spark->>Bronze: Append raw rows
    Spark->>Spark: Parse + validate + normalize
    Spark->>Silver: MERGE valid records
    Spark->>Quarantine: Write invalid records
    Spark->>Proxy: JDBC writes through pooled connections
    Proxy->>Aurora: Upsert product-serving rows
    Spark->>Silver: OPTIMIZE recent as_of_date partition
    Spark->>Ops: Emit job duration, write latency, quarantine rate
```

### 10k Operating Notes

- The process task runs on a Databricks auto-scaling cluster.
- RDS Proxy protects Aurora from too many concurrent Spark JDBC connections.
- Delta `OPTIMIZE` runs after the write to compact small files in the active
  partition.
- Leading bottleneck signals are Databricks job duration, Aurora write latency,
  and quarantine-rate spikes.

## Scale 1m: Event-Driven Streaming

At 1,000,000 clients, the single daily ZIP is the wrong abstraction. The updated
design accepts one client-ready event at a time, lands raw JSON immediately, and
lets Auto Loader feed a Structured Streaming job.

```mermaid
sequenceDiagram
    autonumber
    participant Partner as Partner Webhook
    participant Gateway as API Gateway
    participant Lambda as Landing Lambda
    participant S3Events as S3 raw/events
    participant Loader as Databricks Auto Loader
    participant Stream as Structured Streaming Job
    participant Bronze as Delta Bronze
    participant Silver as Delta Silver
    participant Quarantine as Delta Quarantine
    participant Proxy as RDS Proxy
    participant Aurora as Aurora Postgres
    participant Gold as Delta Gold
    participant Ops as CloudWatch

    Partner->>Gateway: Send client-ready event
    Gateway->>Gateway: Validate HMAC or JWT
    Gateway->>Lambda: Invoke landing handler
    Lambda->>S3Events: Write raw JSON by partner/event_id boundary
    Loader->>S3Events: Discover files via cloudFiles checkpoint
    Loader->>Stream: Deliver micro-batch exactly once per checkpoint
    Stream->>Bronze: Append raw event
    Stream->>Stream: Parse + validate + dedupe on event_id
    alt event is valid
        Stream->>Silver: MERGE accounts, holdings, transactions
        Stream->>Proxy: Upsert current product-serving rows
        Proxy->>Aurora: Pooled Aurora writes
    else event has invalid records
        Stream->>Quarantine: Write raw payload + errors
    end
    Stream->>Ops: Emit backlog, stream lag, trigger duration
    Silver->>Gold: Nightly Workflow OPTIMIZE + current portfolio build
```

### 1m Operating Notes

- The raw S3 event path is the replay checkpoint. A bad deploy can be replayed
  from `raw/events/partner=/event_id=/`.
- Auto Loader's checkpoint owns file discovery and prevents committed files from
  being reprocessed during normal operation.
- Delta `MERGE` on `event_id` provides idempotency at the table layer.
- Health is measured by backlog, stream lag, trigger duration, Aurora latency,
  and quarantine count.

## Validation Subflow

The same validation semantics apply in both batch and streaming modes. This maps
directly to `src/validate/validators.py`.

```mermaid
sequenceDiagram
    autonumber
    participant Parser as JSON Parser
    participant Entry as validate_client_file()
    participant Account as AccountValidator
    participant Holding as HoldingValidator
    participant Txn as TransactionValidator
    participant Result as ValidationResult
    participant Valid as Silver + Aurora
    participant Quarantine as Delta Quarantine

    Parser->>Entry: Parsed client payload
    Entry->>Result: Initialize client result
    loop accounts[]
        Entry->>Account: Validate value, currency, required fields
        Account-->>Result: Append valid account or quarantine error
    end
    loop holdings[]
        Entry->>Holding: Validate quantity, buyPrice, security identifier
        Holding-->>Result: Append valid holding or quarantine error
    end
    loop transactions[]
        Entry->>Txn: Validate type, quantity, trade date, settle date
        Txn-->>Result: Append valid transaction or quarantine error
    end
    Entry->>Entry: Build valid account and holding ID sets
    Entry->>Result: Re-quarantine surviving records with broken references
    Result->>Valid: Valid accounts, holdings, transactions
    Result->>Quarantine: Raw invalid records + errors + reason
```

Key decisions:

- Validate at the ingestion boundary, before database writes.
- Validate accounts, holdings, and transactions independently.
- Run referential-integrity checks after field validation, using only surviving
  valid IDs.
- Preserve raw payloads, entity type, error details, and quarantine reason for
  data-quality review and replay.

## Skeleton Code Map

The starter code under `src/` follows the same boundaries:

| File | Role |
| --- | --- |
| `src/validate/validators.py` | Concrete boundary validator and demo payload. |
| `src/ingest/config.py` | Naming and settings for batch and event paths. |
| `src/ingest/partner_client.py` | Partner API interface for auth, request, poll, download. |
| `src/ingest/storage.py` | S3-style object-store interface and local filesystem adapter. |
| `src/ingest/batch_pipeline.py` | 1k/10k daily workflow skeleton from request to validation. |
| `src/ingest/streaming_pipeline.py` | 1m webhook landing and landed-event processing skeleton. |
| `src/ingest/sinks.py` | Delta, Aurora, and quarantine sink interfaces. |
| `src/ingest/reconcile.py` | Counts, control totals, and alert metrics skeleton. |
