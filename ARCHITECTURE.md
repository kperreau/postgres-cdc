# Architecture

## Overview

postgres-cdc is a single-binary PostgreSQL Change Data Capture service that reads
WAL changes via logical replication (pgoutput) and publishes normalized CDC events
to Redpanda/Kafka topics. It is designed to run as one active instance per
replication slot in a Kubernetes environment.

## Design Principles

1. **Correctness over micro-optimizations** — never checkpoint before durable publish.
2. **Bounded everything** — queues, buffers, in-flight records all have configurable limits.
3. **Generic envelope** — the CDC connector emits a stable, replay-friendly JSON envelope.
   Sink-specific transformations happen downstream.
4. **Crash recovery** — on restart, resume from the last checkpointed LSN. Duplicates
   are expected and acceptable (at-least-once).
5. **Observable** — Prometheus metrics, structured JSON logging, health endpoints.

## Component Map

```
┌─────────────────────────────────────────────────────────────────┐
│                         cmd/cdc                                  │
│  bootstrap: config → logger → metrics → health → pipeline → run  │
└──────────────────────────────┬────────────────────────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
   internal/config      internal/metrics     internal/health
   YAML + env config    promauto registry    /livez + /readyz
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                      internal/pipeline                            │
│                                                                    │
│  ┌──────────┐    ┌────────────┐    ┌──────────┐    ┌───────────┐  │
│  │ pgrepl   │───▶│ txbuffer   │───▶│ encoder  │───▶│ producer  │  │
│  │ WAL read │    │ tx assemble│    │ JSON     │    │ franz-go  │  │
│  └──────────┘    └────────────┘    └──────────┘    └─────┬─────┘  │
│       │                                                   │       │
│       │              ┌───────────┐                        │       │
│       └─────────────▶│ snapshot  │                        │       │
│                      └───────────┘                        │       │
│                                                           ▼       │
│                                                  ┌─────────────┐  │
│                                                  │ checkpoint  │  │
│                                                  │ persist LSN │  │
│                                                  └─────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

## Data Flow

1. **pgrepl** establishes a replication connection, reads WAL messages, parses
   pgoutput protocol messages, and maintains a relation cache.
2. **txbuffer** collects row changes between BEGIN and COMMIT into complete
   transactions. Enforces a per-transaction byte limit.
3. **encoder** converts each `Change` into a `CDCEnvelope` with stable JSON
   serialization and deterministic message keys.
4. **topic** resolves the target Redpanda topic from table metadata
   (e.g., `cdc.app.public.users`).
5. **producer** publishes batches to Redpanda with configurable compression,
   linger, acks, and bounded in-flight records.
6. **checkpoint** persists the last safely-published LSN only after all records
   in a committed transaction are durably acknowledged by Redpanda.
7. On restart, pgrepl resumes streaming from the checkpointed LSN.

## Delivery Semantics

**At-least-once.** A transaction's LSN is checkpointed only after every record
in that transaction has been acknowledged by Redpanda. If the process crashes
after publish but before checkpoint, the transaction will be re-read and
re-published on restart.

## Event Envelope

```json
{
  "source": "postgres-main",
  "database": "app",
  "schema": "public",
  "table": "users",
  "op": "u",
  "txid": 123456,
  "lsn": "0/16B6A30",
  "commit_ts": "2026-03-17T15:00:00Z",
  "snapshot": false,
  "key": { "id": 42 },
  "before": { "id": 42, "email": "old@example.com" },
  "after":  { "id": 42, "email": "new@example.com" }
}
```

## Topic Strategy

Default mode is **per_table**:
```
cdc.{database}.{schema}.{table}
```

A **single** topic mode is available for simple deployments.

## HA Model

- One active instance per replication slot.
- Active/passive deployment. Use Kubernetes leader election (Lease API) to
  ensure only one pod is actively streaming.
- On failover, the new leader resumes from the stored checkpoint and the
  replication slot's confirmed_flush_lsn.

## Backpressure

All internal channels are bounded. If the producer cannot keep up:
1. The encode stage blocks on a full publish channel.
2. The txbuffer stage blocks on a full encode channel.
3. The WAL reader blocks on a full txbuffer channel.
4. PostgreSQL's replication protocol naturally handles slow consumers via
   standby status feedback.

Metrics (`cdc_queue_depth`, `cdc_backpressure_seconds`) expose this state.

## Failure Handling

| Scenario                      | Behavior                                               |
|-------------------------------|--------------------------------------------------------|
| PostgreSQL disconnect         | Reconnect with exponential backoff                     |
| Redpanda disconnect           | Producer retries with backoff; pipeline blocks          |
| Crash before checkpoint       | Re-read from last checkpoint; duplicates expected       |
| Crash during snapshot         | Restart snapshot from beginning (or resume point)       |
| Slot already exists           | Reuse existing slot                                    |
| WAL retention exceeded        | Log fatal, require operator intervention               |
| Large transaction             | Enforce max_tx_bytes; drop or skip oversized txns       |
| Graceful shutdown             | Flush producer, checkpoint, then exit                   |

## Source vs Downstream Responsibilities

| Concern                    | Source (this service)   | Downstream consumer     |
|----------------------------|------------------------|------------------------|
| CDC normalization          | Yes                    | —                      |
| Stable key extraction      | Yes                    | —                      |
| JSON envelope              | Yes                    | —                      |
| Topic routing              | Yes                    | —                      |
| ClickHouse projection      | —                      | Yes                    |
| Flattening / enrichment    | —                      | Yes                    |
| Final table shaping        | —                      | Yes                    |
