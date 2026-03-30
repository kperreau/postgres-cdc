# postgres-cdc

Production-grade PostgreSQL Change Data Capture service that streams WAL changes to Redpanda/Kafka via logical replication (pgoutput).

Single binary. Linux/Kubernetes-first. At-least-once delivery.

## Features

- **PostgreSQL logical replication** via pgoutput protocol (v2)
- **Normalized CDC events** — stable JSON envelope with before/after payloads, all fields always present
- **Topic-per-table routing** — `cdc.{database}.{schema}.{table}`
- **At-least-once delivery** — checkpoint only after durable publish ack
- **Bounded async publish** — `checkpoint_limit` controls in-flight batches with contiguous checkpoint advancement
- **Bounded memory** — configurable queue capacities and max transaction size
- **Crash recovery** — resume from last checkpointed LSN
- **Initial snapshot** — PK-ordered cursor-based scanning with bounded parallel table snapshots
- **Heartbeat emission** — periodic liveness records to a dedicated topic for lag detection
- **TOAST unchanged handling** — configurable strategy: omit unchanged columns or emit a sentinel value
- **Transaction markers** — optional BEGIN/COMMIT records bracketing each transaction's events
- **Temporary replication slot** — auto-dropped on disconnect, ideal for dev/test/CI
- **Prometheus metrics** — pull model, `/metrics` endpoint
- **Health endpoints** — `/livez` and `/readyz` for Kubernetes probes
- **Active/passive HA** — one active instance per replication slot

## Quick Start

```bash
# Start PostgreSQL and Redpanda
docker compose up -d

# Create a publication in PostgreSQL
docker compose exec postgres psql -U cdc -d app -c \
  "CREATE PUBLICATION cdc_pub FOR ALL TABLES;"

# Build and run
make build
./bin/cdc -config config.yaml
```

## Configuration

See [`config.yaml`](config.yaml) for all available options. Configuration can be provided via:

1. **YAML file** — `-config path/to/config.yaml`
2. **Environment variables** — prefixed with `CDC_`, using `__` as separator:
   ```
   CDC_POSTGRES__HOST=db.example.com
   CDC_REPLICATION__SLOT_NAME=my_slot
   CDC_REDPANDA__BROKERS=broker1:9092,broker2:9092
   ```

### Key Configuration Options

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `replication` | `temporary_slot` | `false` | Use a temporary replication slot (auto-dropped on disconnect) |
| `replication` | `heartbeat_interval` | `0s` | Heartbeat emission interval (`0s` = disabled) |
| `replication` | `transaction_markers` | `false` | Emit BEGIN/COMMIT marker records |
| `topic` | `toast_strategy` | `omit` | TOAST unchanged handling: `omit` or `sentinel` |
| `snapshot` | `max_parallel_tables` | `1` | Concurrent table snapshots during initial snapshot |
| `runtime` | `checkpoint_limit` | `1` | Max in-flight tx batches (`1` = sequential) |
| `redpanda.topic_auto_create` | `max_message_bytes` | `0` | Optional per-topic `max.message.bytes` set at topic creation |

## Event Envelope

Every CDC event is a JSON object:

```json
{
  "source": "cdc",
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

Operation types: `c` (insert), `u` (update), `d` (delete), `r` (snapshot read).

### TOAST Unchanged Values

When PostgreSQL does not send the full value of a TOAST column (unchanged on UPDATE), the behavior is controlled by `toast_strategy`:

| Strategy   | Behavior                                                        |
|------------|-----------------------------------------------------------------|
| `omit`     | Column is excluded from `before`/`after` (default)              |
| `sentinel` | Column value is set to the string `"__toast_unchanged"`         |

### Transaction Markers

When `transaction_markers: true`, each transaction's events are bracketed by BEGIN and COMMIT marker records:

```json
{"source":"cdc","database":"app","op":"begin","txid":123,"lsn":"0/16B6A30","commit_ts":"2026-03-17T15:00:00Z","event_count":5}
// ... event records ...
{"source":"cdc","database":"app","op":"commit","txid":123,"lsn":"0/16B6A30","commit_ts":"2026-03-17T15:00:00Z","event_count":5}
```

### Heartbeat

When `heartbeat_interval` is set (e.g. `10s`), periodic heartbeat records are emitted to `{prefix}.__heartbeat`:

```json
{"source":"cdc","database":"app","timestamp":"2026-03-17T15:00:00.000Z","lsn":"0/16B6A30"}
```

This allows downstream consumers to detect liveness and WAL lag even when publication tables are quiet.

## Architecture

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the full design document.

```
PostgreSQL → pgrepl → txbuffer → encoder → producer → Redpanda
                                                ↓
                                           checkpoint
```

## Topic Strategy

| Mode       | Pattern                              | Example                  |
|------------|--------------------------------------|--------------------------|
| `per_table`| `{prefix}.{database}.{schema}.{table}` | `cdc.app.public.users` |
| `single`   | configured topic name                | `cdc-all-events`         |

## Delivery Semantics

**At-least-once.** A transaction's LSN is checkpointed only after every record in that transaction is acknowledged by Redpanda. On crash-restart, duplicates are expected.

When `checkpoint_limit > 1`, up to N transaction batches are published concurrently. The checkpoint LSN only advances contiguously: batch 3 completing before batch 1 does not advance the checkpoint until batch 1 (and then 2) are also acknowledged.

## HA / Kubernetes

- Deploy as a single-replica Deployment or StatefulSet
- One active instance per replication slot
- For leader election, use Kubernetes Lease API
- Health probes: liveness at `/livez`, readiness at `/readyz`
- Metrics scraping at `:9090/metrics`

## Development

```bash
# Run tests
make test

# Run benchmarks
make bench

# Lint
make lint

# Integration test environment
docker compose up -d
make test-integration
```

## Supported / Not Supported

### Supported
- INSERT, UPDATE, DELETE change events
- Initial table snapshot with PK-ordered scanning and parallel tables
- Topic-per-table and single-topic routing
- Configurable compression (snappy, lz4, zstd, gzip)
- Bounded async publish with contiguous checkpoint advancement
- Bounded transaction buffering with oversized tx protection
- Atomic file-based checkpointing
- Heartbeat emission to dedicated topic
- TOAST unchanged value handling (omit or sentinel)
- Optional transaction markers (BEGIN/COMMIT)
- Temporary replication slots for dev/test/CI
- Prometheus pull metrics
- Structured JSON logging
- Helm chart for Kubernetes deployment

### Not Supported (MVP)
- Exactly-once delivery
- Dynamic config reload
- Schema evolution detection
- DDL change events (TRUNCATE logged but not propagated)
- Multi-database CDC
- Sink-specific transformations (by design — use downstream consumers)
