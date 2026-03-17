# postgres-cdc

Production-grade PostgreSQL Change Data Capture service that streams WAL changes to Redpanda/Kafka via logical replication (pgoutput).

Single binary. Linux/Kubernetes-first. At-least-once delivery.

## Features

- **PostgreSQL logical replication** via pgoutput protocol (v2)
- **Normalized CDC events** — stable JSON envelope with before/after payloads
- **Topic-per-table routing** — `cdc.{database}.{schema}.{table}`
- **At-least-once delivery** — checkpoint only after durable publish ack
- **Bounded memory** — configurable queue capacities and max transaction size
- **Crash recovery** — resume from last checkpointed LSN
- **Initial snapshot** — consistent REPEATABLE READ snapshot with cursor-based fetching
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
- Initial table snapshot (mode: `initial`)
- Topic-per-table and single-topic routing
- Configurable compression (snappy, lz4, zstd, gzip)
- Bounded transaction buffering with oversized tx protection
- Atomic file-based checkpointing
- Prometheus pull metrics
- Structured JSON logging

### Not Supported (MVP)
- Exactly-once delivery
- Dynamic config reload
- Schema evolution detection
- DDL change events (TRUNCATE logged but not propagated)
- Multi-database CDC
- Sink-specific transformations (by design — use downstream consumers)
