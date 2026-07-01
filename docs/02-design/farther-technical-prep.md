# Farther Technical Discussion Prep

## 1. Problem Framing

Farther receives daily custodial account data from a partner:

1. Request the day's dataset at 7:00 AM.
2. Poll until the partner marks the request ready.
3. Download a ZIP file.
4. Extract one or more JSON files.
5. Validate and normalize accounts, holdings, and transactions.
6. Store three copies:
  - Raw JSON in S3 for audit and replay.
  - Relational tables in Aurora Postgres for product/API queries.
  - Parquet in S3 for analytics through Glue/Athena.

Main assumption: at 1,000 clients, the hard parts are correctness,
observability, and replayability, not raw scale.

## 2. Baseline Architecture

```
EventBridge 7am schedule
  |
  v
Step Functions orchestration
  |
  +--> Lambda: get OAuth token, call request endpoint
  |
  +--> Step Functions wait/poll loop
  |
  +--> Fargate task: download ZIP to S3 raw/zips/
  |
  +--> Fargate task: unzip to S3 raw/clients/
  |
  +--> SQS queue: one message per client file
  |
  +--> Lambda workers: parse, validate, write DB + Parquet
  |
  v
Reconciler: completeness, control totals, alerts
```

### Why This Shape

| Need | Choice | Reason |
| --- | --- | --- |
| Daily trigger | EventBridge schedule | Simple managed cron for the 7 AM pull. |
| Long-running workflow | Step Functions | The request/poll/download sequence may take minutes and needs visible state, retries, and auditability. |
| Partner API calls | Lambda | Small HTTP calls fit Lambda well and are easy to retry. |
| ZIP download/unzip | ECS/Fargate task | ZIP size is unknown; Fargate avoids Lambda's duration, memory, and temp-storage limits. |
| Per-client fanout | SQS + Lambda | One message per extracted client file gives parallel processing, backpressure, retries, and DLQ handling. |
| Raw replay boundary | S3 | Store ZIP and extracted JSON before transformation so bad deploys can replay without partner re-request. |
| API serving | Aurora Postgres | Relational access patterns, indexed snapshots, and point-in-time recovery. |
| Analytics lake | S3 Parquet + Glue/Athena | Cheap lake storage and queryable analytics without overbuilding a data platform at 1,000 clients. |

### Authentication

Use OAuth 2.0 client credentials:

- Store `client_id` and `client_secret` in AWS Secrets Manager.
- A Lambda task requests a short-lived partner token before partner calls.
- Cache the token within the Step Functions execution or worker process so every
  client-file worker does not request a new token.
- Add Redis/ElastiCache only if token fetch volume or partner rate limits make
  local execution-level caching insufficient.
- Request narrow scopes such as `read:positions` and `read:transactions`.
- Alert on 401/403 immediately because auth failure blocks the entire workflow.

### Module Breakdown

| Module | Responsibility | Runs on | Boundary after |
| --- | --- | --- | --- |
| Scheduler | Start daily ingest at 7 AM | EventBridge | Step Functions execution |
| Auth/request Lambda | Fetch token, POST dataset request | Lambda | `request_id` |
| Poll loop | Wait until partner dataset is ready | Step Functions | Ready status |
| Download task | Stream partner ZIP to `s3://.../raw/zips/` | ECS/Fargate | Raw ZIP in S3 |
| Extract task | Unzip client JSON files to `raw/clients/` | ECS/Fargate | Raw client files in S3 |
| Fanout publisher | Enqueue one SQS message per client file | Lambda or Fargate | SQS message |
| Worker | Parse, validate, write Aurora + Parquet | Lambda | DB transaction + Parquet write |
| Reconciler | File counts, control totals, metrics, alerts | Lambda or Step Functions task | Completion signal |

## 3. Correctness and Operations

### Idempotency

Each run gets a stable `batch_id`, and each output is keyed by business identity:

```
batch_id = partner_request_id or as_of_date + partner_name
client_file_key = partner_name/as_of_date/client_id/source_file_hash
holding_key = account_id + holding_id + as_of_date
transaction_key = transaction_id
```

The worker should:

- Write account and holding snapshots with upserts on `(account_id, as_of_date)`
  and `(holding_id, as_of_date)`.
- Insert immutable transactions with a unique `transaction_id`.
- Write raw invalid records to a quarantine table or S3 quarantine prefix.
- Treat SQS redelivery as normal. A duplicate message should become a no-op
  because the database and lake writes are keyed idempotently.

### Completeness

A day is complete when:

- The partner request reached `ready`.
- The expected file inventory matches files extracted.
- Every file is either processed or quarantined.
- SQS visible/in-flight messages are zero.
- DLQ depth is zero.
- Account/holding counts and total account value are within expected bands.

Reasonable SLO: daily data available by 9:30 AM ET, with page-level alerts for
auth failures, empty files, DLQ messages, or missing completion.

### Failure Handling

| Failure | Response |
| --- | --- |
| Partner 5xx or timeout | Retry with exponential backoff inside Step Functions. |
| Download interrupted | Re-run the Fargate download task; overwrite the same S3 ZIP key safely. |
| Bad client JSON | Worker quarantines the record and deletes the SQS message so one client does not block the run. |
| Worker crash | SQS visibility timeout returns the message for retry; max receives route to DLQ. |
| Bad deploy | Replay from raw S3 through the fixed processor; no partner re-request required. |
| Aurora pressure | Add RDS Proxy and cap Lambda reserved concurrency. |

## 4. Scaling Path

### 1,000 Clients

This baseline is intentionally simple. The ZIP and extract steps are serial, but
that is acceptable if the file is small enough to hit the morning SLO. The main
operational focus is validating and reconciling cleanly.

### 10,000 Clients

- Keep the EventBridge + Step Functions shape.
- Use larger Fargate tasks for download/extract.
- Increase SQS worker concurrency carefully.
- Add RDS Proxy in front of Aurora to protect connection limits.
- Watch `ApproximateAgeOfOldestMessage`, Lambda duration/error/concurrency, and
  Aurora write latency.
- Compact Parquet files after each batch, because one file per client can become
  a small-file problem.

### 1,000,000 Clients

The single ZIP pattern breaks at this scale. Options:

- Ask the partner for a manifest of shard URLs and run parallel Fargate download
  tasks per shard.
- If the partner can push events as clients become ready, switch to the
  event-driven design below.
- Avoid one Parquet file per client per day. Partition by `as_of_date`, add a
  `client_bucket = hash(client_id) % 1000` sub-partition if needed for parallel
  compaction, and run a Glue or Spark compaction job to produce 128-512 MB files.

## 5. Event-Driven Redesign

When the partner emits events the moment a client's data is ready, the operating
question shifts from "did the batch finish?" to "are we caught up?"

```
Partner webhook
   -> API Gateway (Lambda authorizer validates HMAC/JWT)
   -> EventBridge event bus (routing, filtering, fan-out)
   -> SQS standard queue + DLQ (buffering, at-least-once delivery)
   -> Lambda processor (idempotent per-event worker)
   -> S3 raw + Aurora + Parquet
```

### Q1: Why each service, and where does each break?

| Component | Choice | Reason | Breaks at |
| --- | --- | --- | --- |
| Edge auth | API Gateway + Lambda authorizer | Validates partner token before compute/storage work starts. | High request rates require throttle tuning and reserved concurrency. |
| Event routing | EventBridge | Schema registry, filtering, fan-out, and decoupling. | Very high sustained event rates may push toward Kinesis. |
| Buffering | SQS standard queue + DLQ | Durable parallel delivery, retry through visibility timeout, DLQ for poison messages. | Standard queue is unordered; use FIFO/message group by `client_id` if ordering matters. |
| Compute | Lambda | Per-event invocation and automatic scaling. | Reserved concurrency may be needed to avoid account throttling and Aurora overload. |
| Storage | S3 + Aurora + Parquet | Raw replay boundary, product serving, and analytics layout stay unchanged. | Aurora connection saturation requires RDS Proxy or a lower-concurrency worker model. |

### Q2: No double-processing - where does the dedupe key live?

Use `(client_id, event_id)` from the partner webhook payload.

The Lambda processor checks that key before doing work:

- `processed_events(client_id, event_id)` in Aurora with a unique constraint, or
  DynamoDB with conditional writes and TTL if the event table is purely
  operational.
- SQS visibility timeout prevents normal concurrent processing of the same
  message while one worker is active.
- Aurora upserts on `(account_id, as_of_date)`, `(holding_id, as_of_date)`, and
  `transaction_id` provide a second idempotency layer.
- If a duplicate event is redelivered, the processor exits as a no-op.

### Q3: Observability - healthy vs. falling behind

Healthy signals:

- `ApproximateAgeOfOldestMessage` near zero.
- Messages sent roughly match messages deleted.
- Lambda error rate near zero.
- DLQ depth zero.
- Last processed event per client/account is within the freshness SLO.

Falling-behind signals:

- SQS queue age rising.
- Lambda concurrency approaching reserved or account limits.
- Lambda duration rising.
- Aurora write latency increasing.
- Duplicate event rate or quarantine rate spiking.

DLQ alarm strategy:

- Page when DLQ depth is greater than zero for production ingestion.
- Include partner, event ID, client ID, failure reason, and raw S3 key in the
  dead-letter payload or logs.
- Build a replay path that can redrive DLQ messages after the processor is fixed.

### Q4: Replay after a bad deploy

Every event or client file is stored raw in S3 before processing. That is the
replay checkpoint.

Replay process:

1. Fix the bug and deploy the processor.
2. List raw S3 objects for the affected time window or batch.
3. Re-enqueue each raw object key as a new SQS message.
4. Reprocess idempotently through the same worker.
5. If the bug wrote incorrect processed markers, clear or bypass the relevant
   `processed_events` rows for the affected keys only.

No partner re-request is needed when the raw S3 boundary was written correctly.

### Q5: Regional outage - blast radius and remediation

Blast radius:

- Query `last_ingested_at` to identify clients stale since the outage start.
- Check SQS in-flight and DLQ messages.
- Confirm Aurora state through point-in-time recovery.
- Check which raw S3 objects landed before the outage and were not processed.

Remediation:

- Short outage: SQS redelivery and Lambda retry process pending messages after
  recovery.
- Full regional failure: raw S3 should use cross-region replication if the RPO
  requires it. Otherwise, recovery waits for the region or partner re-emission.
- Aurora PITR gives database recovery, but replay from raw S3 is still needed to
  rebuild downstream lake/product state.

### Q6: How event-driven changes lake partitioning vs daily cron

Cron design:

- Partition snapshots by `as_of_date`.
- The full day lands as one logical batch.
- Completeness is batch-based.

Event-driven design:

- Partition by `event_date` or `ingestion_hour`.
- Late events arrive continuously and may be out of order.
- Completeness means no queue messages older than a threshold, not "the day is
  atomically done."
- Use SQS FIFO with `client_id` as message group only if strict per-client
  ordering matters.
- Parquet compaction becomes a recurring background job rather than one final
  batch task.

## 6. Data Model

The sample JSON exposes the important modeling issues:

- `value` arrives as a string and must be coerced.
- Holdings are point-in-time snapshots.
- Transactions are immutable events.
- `security` is nullable, especially for fixed income, so ticker alone is not a
  sufficient security model.
- Currency matters for aggregation.

### Relational Core

Use Postgres/Aurora for product queries:

```sql
clients(
  client_id primary key,
  name,
  created_at,
  updated_at
)

accounts_snapshot(
  account_id,
  client_id,
  as_of_date,
  name,
  type,
  value_amount,
  currency,
  primary key (account_id, as_of_date)
)

holdings_snapshot(
  holding_id,
  account_id,
  client_id,
  as_of_date,
  name,
  ticker,
  cusip,
  isin,
  security_type,
  quantity,
  buy_price,
  is_cash_like,
  primary key (holding_id, as_of_date)
)

transactions(
  transaction_id primary key,
  account_id,
  holding_id,
  type,
  quantity,
  value_amount,
  trade_date,
  settle_date
)
```

Important indexes:

- `accounts_snapshot(client_id, as_of_date)`
- `holdings_snapshot(account_id, as_of_date)`
- `holdings_snapshot(client_id, as_of_date)`
- `holdings_snapshot(ticker)` where ticker is not null
- `transactions(account_id, trade_date)`

### Lake Layout

```
s3://farther-data-lake/
  raw/
    partner=custodian/as_of_date=2026-06-29/client_id=c_1234/source.json
  normalized/
    accounts/as_of_date=2026-06-29/
    holdings/as_of_date=2026-06-29/
    transactions/trade_date=2026-06-29/
  quarantine/
    as_of_date=2026-06-29/
```

Use Glue Catalog tables over the Parquet layout and Athena for ad hoc analytics.
At larger scale, compact small files and prefer date-level partitions with
bucketed sub-partitions only where they improve parallelism.

### Agent and Business Discoverability

Do not let agents write arbitrary SQL against raw tables by default. Give them:

- Documented views such as `v_client_portfolio_current`.
- Narrow tools such as `get_client_holdings(client_id, as_of_date)`.
- Column descriptions through dbt docs or Glue catalog.
- Clear freshness metadata: source partner, `as_of_date`, `ingested_at`, and
  validation status.

## 7. Schema Evolution

Expect custodian schemas to differ. Keep raw payloads forever and normalize into
a canonical model.

Practical rules:

- Add nullable columns for common new fields.
- Keep partner-specific overflow in `metadata JSONB` when needed.
- Version the raw schema and record `source_schema_version`.
- Avoid Postgres enums for fast-changing partner values like account type.
- Use dbt/Glue docs so downstream consumers can see what changed.

## 8. Validation

Validation happens in the Lambda worker after raw JSON is stored and before
Aurora or Parquet writes. Bad records are quarantined, not dropped, and the
pipeline continues processing the remaining clients.

```
Raw client JSON from S3
        |
        v
Parser / schema enforcement
        |
        v
Business rule validators
        |
        +--> valid records -> Aurora upsert + Parquet write
        |
        +--> invalid records -> quarantine table/prefix + CloudWatch metric
        |
        v
Post-load checks in reconciler
```

Boundary rules:

- Account value must be numeric and non-negative.
- Currency must be in the supported set.
- Non-cash holdings must have at least one security identifier: ticker, CUSIP,
  or ISIN.
- Quantities and buy prices cannot be negative.
- Transaction type must be known.
- `settleDate` cannot be before `tradeDate`.
- Holdings and transactions must reference an account present in the same batch.

Post-load checks:

- Primary key uniqueness on `(holding_id, as_of_date)`,
  `(account_id, as_of_date)`, and `transaction_id`.
- Non-null required fields.
- Referential integrity between holdings, accounts, and clients.
- Accepted values for `transaction_type` and `currency`.
- Freshness within the SLO window.
- Statistical checks for client count, total account value, and quarantine rate.

## 9. Build Estimate

For a production-quality first version:

- 1-2 weeks: partner API client, OAuth, Step Functions orchestration, S3
  boundaries.
- 1 week: parser, validators, quarantine flow, Aurora writes.
- 1 week: Parquet/lake output, reconciliation, dashboards, alerts.
- 1 week: replay tooling, integration tests, hardening.

Biggest unknowns:

- Partner API reliability and file size.
- Exact schema variance across custodians.
- Whether the partner provides file manifests or control totals.
- Required SLO and operational support expectations.

Lowest complexity: daily schedule, raw S3 storage, simple relational schema.

Highest complexity: idempotent replay, schema evolution, completeness
reconciliation, and diagnosing partner data quality failures.
