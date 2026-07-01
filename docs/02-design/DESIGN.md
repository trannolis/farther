# Farther Technical Discussion Prep

## Custodial Data Ingestion Pipeline

This is the answer I would defend live. The goal is not to design every future
system up front; it is to show a simple baseline, name the scaling limits, and
explain how validation keeps bad partner data from becoming bad client data.

## 1. Problem Framing

Farther receives daily custodial account data from a partner:

1. Request the day's dataset at 7:00 AM.
2. Poll until the partner marks the request ready.
3. Download a ZIP file.
4. Extract one or more JSON files.
5. Validate and normalize accounts, holdings, and transactions.
6. Store three copies:
  - Raw JSON in S3 for audit and replay.
  - Relational tables for product/API queries.
  - Parquet in the lake for analytics.

Main assumption: at 1,000 clients, the hard parts are correctness,
observability, and replayability, not raw scale.

## 2. Baseline Architecture

```
Databricks Workflows (7am scheduled trigger)
        |
        +--> Task 1: auth_notebook
        |    OAuth 2.0 — token from Databricks Secret Scope (backed by Secrets Manager)
        |
        +--> Task 2: request_notebook
        |    POST to partner endpoint → receive request_id
        |
        +--> Task 3: poll_notebook (retries with exponential backoff)
        |    Poll status endpoint until ready
        |
        +--> Task 4: download_notebook
        |    Stream ZIP from partner URL → S3 raw/zips/batch_id=   [boundary]
        |
        +--> Task 5: extract_notebook
        |    Unzip → S3 raw/clients/as_of_date=/client_id=          [boundary]
        |
        +--> Task 6: process_notebook  (Spark job)
        |    Read all client JSON from S3 in parallel
        |    Parse + validate (schema enforcement + business rules)
        |    Quarantine invalid records → Delta quarantine table
        |    Write valid records:
        |      → Delta Lake Bronze (raw) → Silver (normalized)
        |      → Aurora Postgres via JDBC (product API queries)
        |
        +--> Task 7: reconcile_notebook
             File count, control totals, CloudWatch custom metrics + alerts
```

### Why This Shape

| Need | Choice | Reason |
| --- | --- | --- |
| Daily trigger | Databricks Workflows scheduler | Native job scheduling; no EventBridge needed. |
| Long-running workflow | Databricks Workflows tasks | Task-level retries, dependencies, audit trail in the Workflows UI. |
| Partner API calls | Databricks notebook (Python) | Simple HTTP — runs in the same cluster, no extra service. |
| ZIP download/unzip | Databricks notebook | Cluster has RAM and disk; streams directly to S3 via boto3 or DBFS mount. |
| Parallel per-client processing | Spark (native Databricks) | Replaces SQS + Lambda fan-out — Spark partitions the file list and processes clients in parallel across workers. |
| Raw replay boundary | S3 | Every downstream step can be replayed from source. |
| Lake storage | Delta Lake on S3 | ACID transactions, time travel for replay, schema evolution, compaction built in. |
| API serving | Aurora Postgres via JDBC | Relational access patterns, point-in-time queries. |

### Authentication

Use OAuth 2.0 client credentials:

- Store `client_id` and `client_secret` in a Databricks Secret Scope backed by
  AWS Secrets Manager.
- Fetch the token once in the auth task; pass it as a job parameter to
  downstream tasks so the cluster does not re-request it on every API call.
- Request narrow scopes such as `read:positions` and `read:transactions`.
- Alert on 401/403 immediately — auth failure blocks the entire workflow.

### Module Breakdown

| Module | Responsibility | Runs on | Boundary after |
| --- | --- | --- | --- |
| Scheduler | Databricks Workflows 7 AM cron | Managed | — |
| auth_notebook | OAuth token fetch from Secret Scope | Databricks cluster | — |
| request_notebook | POST to partner, receive `request_id` | Databricks cluster | — |
| poll_notebook | Retry loop on status endpoint | Databricks cluster | — |
| download_notebook | Stream ZIP to `S3 raw/zips/` | Databricks cluster | S3 write |
| extract_notebook | Unzip → `S3 raw/clients/` JSON | Databricks cluster | S3 write |
| process_notebook | Spark: parse, validate, write Delta + Aurora | Databricks cluster (Spark) | Delta ACID commit |
| reconcile_notebook | File counts, control totals, emit CloudWatch metrics | Databricks cluster | — |

All tasks run serially in one Databricks Workflow execution. Parallelism within
the processing step comes from Spark partitioning the client file list across
workers — no SQS or Lambda fan-out needed.

### Boundaries

The key design principle is to create durable S3 boundaries before expensive or
failure-prone work:

- ZIP stored in S3 before unzip. If the cluster dies mid-unzip, the download
  task is not re-run.
- Extracted client JSON stored in S3 before Spark processing. The process task
  can be re-run independently.
- Delta Lake ACID commits mean a partial Spark write does not corrupt the table —
  the transaction either completes or rolls back.
- Invalid records go to a Delta quarantine table; they do not block valid clients
  from loading.

## 3. Correctness and Operations

### Idempotency

Each run gets a `batch_id` and each output is keyed by stable business identity:

```
batch_id = partner_request_id or as_of_date + partner_name
client_file_key = partner_name/as_of_date/client_id/source_file_hash
holding_key = account_id + holding_id + as_of_date
transaction_key = transaction_id
```

Database writes should use upserts on snapshot tables and insert-ignore or
natural-key constraints on immutable transactions. Replays from S3 should produce
the same final state.

### Completeness

A day is complete when:

- The partner request reached `ready`.
- The expected file inventory matches files extracted.
- Every file is either processed or quarantined.
- DLQ depth is zero.
- Account/holding counts and total account value are within expected bands.

Reasonable SLO: daily data available by 9:30 AM ET, with page-level alerts for
auth failures, empty files, DLQ messages, or missing completion.

### Failure Handling

| Failure | Response |
| --- | --- |
| Partner 5xx or timeout | Databricks task retry with exponential backoff (configured per-task in Workflows UI). |
| Download interrupted | Re-run download_notebook; overwrites same S3 key safely. |
| Bad client JSON | Quarantine record to Delta table; Spark continues processing remaining clients. |
| Bad deploy | Re-run process_notebook against the existing raw S3 files; Delta MERGE is idempotent. |
| Cluster failure mid-job | Databricks restarts the failed task from the last S3 boundary checkpoint. |


## 4. Scaling Path

At 1,000 clients, the baseline is intentionally boring. At larger scale, the
first bottleneck is the single ZIP/unzip step because it serializes delivery.

### 10,000 Clients

- Same Databricks Workflows shape.
- Enable Databricks cluster auto-scaling (min/max workers) so the process task
  scales with the file count automatically.
- Add RDS Proxy in front of Aurora — Spark opens many JDBC connections
  concurrently and will exhaust the Aurora connection limit without pooling.
- Run `OPTIMIZE` on Delta tables after each process task to compact small files.
- Watch Databricks job run duration and Aurora write latency as the leading
  bottleneck indicators.

### 1,000,000 Clients

The single-ZIP pattern breaks entirely at this scale — one cluster cannot
download and unzip a ZIP containing 1M client files within the SLO.

If the partner supports a manifest, download partitions in parallel using
multiple Databricks Workflow tasks running concurrently (one per shard):

```json
{
  "as_of_date": "2026-06-29",
  "files": [
    {"prefix": "00-0f", "url": "https://partner/batch-00.zip", "count": 40000},
    {"prefix": "10-1f", "url": "https://partner/batch-10.zip", "count": 41000}
  ]
}
```

If the partner supports real-time events (freshness over batch completeness),
move to a streaming architecture — see Section 5.

### Lake Partitioning at 1,000,000 Clients

Delta Lake handles small-file compaction natively via `OPTIMIZE` and `ZORDER`,
which eliminates the Glue compaction job needed with raw Parquet.

- Partition by `as_of_date` only; Delta `OPTIMIZE` runs after each batch write
  and merges small files within the partition automatically.
- For event-driven ingestion, add an `ingestion_hour` sub-partition so
  `OPTIMIZE` can run on recent partitions without scanning the full table.
- Partition transactions by `trade_date`.
- Target file size: 128–512 MB (Delta default is 1 GB; tune down for faster
  `OPTIMIZE` on daily partitions).

## 5. Event-Driven Redesign

When the partner emits events the moment a client's data is ready, the batch
Workflows pipeline is replaced with a Databricks Structured Streaming job using
Auto Loader. The operating question shifts from "did the batch finish?" to
"are we caught up?"

```
Partner webhook
   -> API Gateway (Lambda authorizer validates partner HMAC/JWT)
   -> Lambda (lightweight landing — writes raw JSON to S3 raw/events/)  [boundary]
   -> S3 event notification
   -> Databricks Auto Loader (cloudFiles source, watches S3 prefix)
   -> Databricks Structured Streaming job
        |- Bronze: write raw event to Delta table (exactly-once via checkpoint)
        |- Silver: parse, validate, MERGE into normalized Delta tables
        |- JDBC sink: upsert to Aurora Postgres
   -> Databricks Workflows (scheduled): OPTIMIZE + Aurora sync reconciliation
```

The Lambda is kept intentionally thin — its only job is durable S3 landing.
All parsing, validation, and writing happens in the Structured Streaming job.

### Q1: Why each service, and where does each break?

| Component | Choice | Reason | Breaks at |
| --- | --- | --- | --- |
| Edge auth | API Gateway + Lambda authorizer | Validates partner HMAC/JWT before anything is stored | ~10k req/s without throttle tuning |
| Landing | Lambda → S3 | Lightweight; decouples partner delivery from processing speed | Lambda 15-min limit is fine — it only writes one file |
| Streaming ingest | Databricks Auto Loader | Native S3 file discovery with exactly-once guarantees; no SQS needed | Scales to millions of files; checkpoint state in DBFS/S3 |
| Processing | Databricks Structured Streaming | Micro-batch Spark; parallelism scales with cluster size | Cluster size is the knob — add workers as event rate grows |
| Idempotency | Delta Lake MERGE on `event_id` | Built-in deduplication without a separate DynamoDB table | MERGE cost grows with table size — Z-ORDER on `event_id` |
| Lake storage | Delta Lake | ACID, time travel for replay, compaction built in | Same S3 costs; no Glue needed |
| API serving | Aurora Postgres + RDS Proxy | Unchanged relational model | RDS Proxy required — streaming opens concurrent JDBC connections |

### Q2: No double-processing — where does the dedupe key live?

Delta Lake handles this natively. The MERGE statement keys on `event_id`:

```sql
MERGE INTO silver.holdings AS target
USING incoming AS source ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

Auto Loader's checkpoint file in S3/DBFS tracks which S3 files have been
processed — the stream never re-reads a committed file unless the checkpoint
is explicitly reset. This replaces the DynamoDB `processed_events` table and
the SQS visibility timeout that were doing this job in the Lambda architecture.

### Q3: Observability — healthy vs. falling behind

**Healthy signals:** stream lag near zero, Auto Loader backlog near zero, last
processed `event_id` per client is recent.

**Falling behind signals:** Databricks streaming backlog metric rising, trigger
interval taking longer than the configured interval, Aurora JDBC write latency
increasing.

Key metrics:
- Databricks streaming backlog (number of pending S3 files in Auto Loader queue)
  — primary "falling behind" signal; alert if > N files older than 15 minutes.
- Stream trigger duration — if it exceeds the trigger interval, the job is
  falling behind on each micro-batch.
- Delta table `last_commit_timestamp` per Bronze/Silver table — freshness check.
- Aurora write latency via CloudWatch RDS metrics.
- Custom metric: events processed per client per hour (emit from the Spark job).

No DLQ in this architecture. Failed records go to a Delta quarantine table
(write-once, ACID). Alert at `quarantine_count > 0` for a given batch window.

### Q4: Replay after a bad deploy

Every event is stored raw in S3 at `raw/events/partner=X/event_id=Y/` before
the stream processes it. This is the replay checkpoint.

Replay process:
1. Fix the bug and deploy the corrected streaming job.
2. Reset the Auto Loader checkpoint to the affected time window's start
   (`cloudFiles.setCheckpointToTime` or delete checkpoint and re-specify).
3. The stream re-reads from that S3 prefix and re-processes.
4. Delta MERGE is idempotent — re-processing the same `event_id` is a no-op.

No partner re-request and no re-enqueue script needed — the raw S3 landing and
the Auto Loader checkpoint together make replay a single configuration change.

### Q5: Regional outage — blast radius and remediation

**Blast radius:**
- Query Delta table for `last_ingested_at < outage_start` to identify affected
  clients.
- Check Auto Loader checkpoint: files that landed in S3 before the outage but
  after the last checkpoint commit are pending and will be re-processed on
  recovery (at-least-once, MERGE makes it safe).
- Confirm Aurora state via RDS point-in-time recovery.

**Remediation:**
- Short outage: Auto Loader resumes from checkpoint automatically on cluster
  restart. S3 is durable so no events are lost if they landed before the outage.
- Full regional failure: S3 Cross-Region Replication can be enabled so raw
  events survive. Start the streaming cluster in a secondary region pointing at
  the replicated bucket.

### Q6: How event-driven changes lake partitioning vs daily cron

Cron design: partition by `as_of_date`. The full day lands atomically. Ordering
is guaranteed — the Spark job processes all files in one run.

Structured Streaming design:
- Partition by `event_date` and `ingestion_hour` — micro-batches land
  continuously, not atomically per day.
- Late-arriving events (partner re-sends a corrected record) are handled by
  Delta MERGE, which overwrites the prior version — no manual backfill needed.
- No "day is complete" signal — completeness is defined as "Auto Loader backlog
  is zero and the last commit is within the SLO window."
- Per-client ordering within the stream is not guaranteed across Spark
  partitions. If strict ordering matters, key the stream on `client_id` so
  all events for one client land on the same executor.
- `OPTIMIZE` runs on a schedule (Databricks Workflow nightly) rather than
  per-batch, targeting recent partitions only.

## 6. Data Model

The sample JSON already shows the important modeling issues:

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

Delta Lake replaces raw Parquet. The medallion layout maps directly to
Databricks best practices:

```
s3://farther-data-lake/
  bronze/
    raw_events/partner=custodian/as_of_date=2026-06-29/       ← Delta table, raw JSON
  silver/
    accounts/as_of_date=2026-06-29/                           ← Delta table, validated snapshots
    holdings/as_of_date=2026-06-29/
    transactions/trade_date=2026-06-29/
    quarantine/as_of_date=2026-06-29/                         ← invalid records
  gold/
    client_portfolio_current/                                  ← pre-aggregated for API/agents
```

Partition snapshots by `as_of_date`. Partition transactions by `trade_date`
because they are business events, not daily state. Delta time travel replaces
the need for a separate raw S3 archive — `VERSION AS OF` or `TIMESTAMP AS OF`
lets any prior snapshot be recovered without a separate copy.

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

Validation happens inside the Databricks process_notebook (batch) or the
Silver-layer Structured Streaming job (event-driven), before records reach
Aurora or the Silver Delta table. Bad records are quarantined, not dropped —
the pipeline continues processing the remaining clients.

```
Bronze Delta table (raw JSON)
        |
        v
Spark schema enforcement (StructType — rejects unparseable records)
        |
        v
Business rule validators (Python UDFs or Spark DataFrame filters)
        |
        +--> valid records → Silver Delta (MERGE) + Aurora JDBC upsert
        |
        +--> invalid records → quarantine Delta table + CloudWatch metric
        |
        v
Post-load expectations (Great Expectations or dbt tests on Silver tables)
```

### Boundary Rules

Enforced in the Spark processing step via DataFrame validation:

- Account value must be numeric and non-negative.
- Currency must be in the supported set.
- Non-cash holdings must have at least one security identifier (ticker, CUSIP, or ISIN).
- Quantities and buy prices cannot be negative.
- Transaction type must be a known enum value.
- `settleDate` cannot be before `tradeDate`.
- Holdings and transactions must reference an account present in the same batch.

### Post-Load Rules

Run as a Great Expectations suite (or dbt tests) against the Silver Delta tables
after each batch or on a scheduled Databricks Workflow task:

- Primary key uniqueness on `(holding_id, as_of_date)`, `(account_id, as_of_date)`, `transaction_id`.
- Non-null required fields.
- Referential integrity between holdings → accounts → clients.
- Accepted values for `transaction_type` and `currency`.
- Freshness: Silver table `last_commit_timestamp` within the SLO window.

Statistical checks (run in reconcile_notebook):

- Client count within ±5% of prior day.
- Total account value within a tolerance band of prior day.
- Quarantine rate spiked for a partner (may indicate a schema change).

## 9. Build Estimate

For a production-quality first version:

- 1-2 weeks: partner API client, auth, orchestration, S3 boundaries.
- 1 week: parser, validators, quarantine flow, relational writes.
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