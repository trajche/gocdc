# gocdc

Config-driven Change Data Capture from MySQL/MariaDB to multiple sinks. Single binary, no broker, no Java.

```
MariaDB/MySQL (binlog ROW) → gocdc → PostgreSQL / Redis / stdout
```

## Install

Download a binary from [Releases](https://github.com/trajche/gocdc/releases), or build from source:

```bash
go install github.com/trajche/gocdc@latest
```

## Quick Start

```bash
cp config.example.yaml config.yaml
# Edit config.yaml with your source/sink details
gocdc -config config.yaml
```

First run performs a snapshot (chunked `SELECT` of all configured tables), then switches to binlog streaming. Subsequent runs resume from the saved checkpoint.

## How It Works

1. **Load config** — parses `config.yaml`, resolves column ordinals via `DESCRIBE` on each source table
2. **Snapshot** — per-table chunked `SELECT ... WHERE pk > ? ORDER BY pk LIMIT N` with optional `snapshot_filter`
3. **Stream** — binlog replication via [canal](https://github.com/go-mysql-org/go-mysql), filtered per-table by `stream_filter`
4. **Batch + flush** — rows are batched (by count or timer) then fanned out to all configured sinks
5. **Checkpoint** — binlog position saved every 5s to `checkpoint.json` for resumption

## Config

See [`config.example.yaml`](config.example.yaml) for a full annotated example.

### Source

```yaml
source:
  addr: "127.0.0.1:3306"
  user: "root"
  password: ""
  database: "mydb"
  flavor: "mariadb"   # or "mysql"
  server_id: 101
```

Requires binlog in ROW format (`binlog_format = ROW`).

### Sinks

Multiple sinks can be configured — every event is written to all of them.

| Type | Storage | Use Case |
|------|---------|----------|
| `postgres` | SQL table with upsert | Analytics, joins, complex queries |
| `redis` | Hash per row (`HSET`) | Fast key-value lookups, cache |
| `redis-json` | JSON string per row (`SET`) | Apps preferring `GET` + JSON parse |
| `stdout` | JSON lines | Debugging, piping to other tools |

```yaml
sinks:
  - type: "postgres"
    dsn: "postgres://user@localhost:5432/db?sslmode=disable"
    schema: "cdc"

  - type: "redis"
    addr: "127.0.0.1:6379"
    prefix: "cdc"

  - type: "stdout"
```

**PostgreSQL** — auto-creates tables with TEXT columns + `_cdc_operation` and `_cdc_timestamp`. Uses `INSERT ... ON CONFLICT DO UPDATE` for upserts.

**Redis** — keys are `{prefix}:{table}:{pk}`. Hash sink uses `HSET`, JSON sink uses `SET` with a JSON string.

**stdout** — one JSON line per event, useful for `gocdc | jq .` or piping to other consumers.

### Tables

```yaml
tables:
  - source: "wp_posts"              # MySQL table name
    target: "products"              # sink table/key name
    primary_key: ["ID"]
    columns:
      - { source: "ID", target: "id" }
      - { source: "post_title", target: "title" }
    snapshot_filter: "post_type = 'product'"  # SQL WHERE for initial snapshot
    stream_filter:                             # binlog row filter
      column: "post_type"
      values: ["product", "product_variation"]
      # pattern: "prefix_%"                   # alternative: SQL LIKE match
```

## CLI Flags

| Flag | Description |
|------|-------------|
| `-config` | Path to config file (default: `config.yaml`) |
| `-snapshot-only` | Run snapshot then exit |
| `-no-snapshot` | Skip snapshot, start streaming from checkpoint |

## Adding a New Table

1. Add an entry to `config.yaml` with source/target, columns, PK, and optional filters
2. Create the target table in PostgreSQL (or let gocdc auto-create it with TEXT columns)
3. Restart gocdc (or delete `checkpoint.json` to re-snapshot)

No code changes needed.

## Requirements

- MySQL/MariaDB with `binlog_format = ROW`
- A user with `REPLICATION SLAVE` and `REPLICATION CLIENT` privileges
- Go 1.22+ (build only)
