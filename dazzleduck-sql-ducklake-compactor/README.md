# DazzleDuck DuckLake Compaction Service

A background service that runs minor and major compaction on DuckLake catalogs to keep Parquet file counts manageable and reclaim storage from expired snapshots.

## Overview

DuckLake writes small Parquet files on each insert/update. Without compaction, query performance degrades as the file count grows. This service runs two compaction strategies on a schedule:

- **Minor compaction** — merges adjacent small files using `ducklake_merge_adjacent_files`. Runs frequently (default: every 1 minute).
- **Major compaction** — merges all files, expires old snapshots, and cleans up deleted files. Runs less frequently (default: every 1 hour).

## Configuration

All settings live under the `dazzleduck_sql_compaction` HOCON root in `application.conf` and can be overridden at runtime with `--conf key=value`.

| Key | Default | Description |
|-----|---------|-------------|
| `databases` | `[]` | DuckLake catalog names to compact (must be attached via startup script) |
| `minor_compaction_frequency` | `1 minute` | How often to run minor compaction |
| `major_compaction_frequency` | `1 hour` | How often to run major compaction |
| `minor_compaction_max_size` | `8MB` | Only merge files smaller than this |
| `major_compaction_max_size` | `64MB` | Only compact files smaller than this during major pass |
| `housekeeping_frequency` | `5 minutes` | How often to expire snapshots and delete orphaned files |
| `snapshot_retention` | `15 minutes` | Expire snapshots older than this during housekeeping |
| `health_port` | `8080` | Port for the `GET /health` endpoint |
| `idle_in_transaction_timeout` | `2 minutes` | Value used the first time a database's idle timeout is escalated (see below) |
| `idle_in_transaction_timeout_max` | `30 minutes` | Escalation ceiling; doubles on each further escalation |
| `idle_in_transaction_timeout_adaptive` | `false` | Global opt-in for idle-timeout escalation |
| `postgres_metadata` | `[]` | Per-catalog connection info needed to escalate that catalog (see below) |
| `startup_script_provider` | — | How to load the startup SQL (attach catalogs, load extensions) |
| `config_provider` | — | Optional: overlay config values read from a table (see below) |

The bundled defaults live in this module's `application.conf` (not `reference.conf`).

### Idle-in-transaction timeout escalation

A long compaction or housekeeping cycle against a Postgres-backed DuckLake catalog holds a Postgres
metadata transaction open while it writes to object storage. If that write outlives Postgres'
`idle_in_transaction_session_timeout`, the server kills the connection, the commit fails, the
already-written Parquet is orphaned, and the next cycle repeats the work.

When `idle_in_transaction_timeout_adaptive = true` and a catalog has a matching `postgres_metadata`
entry, a detected idle-timeout-shaped failure for that catalog causes the compactor to `DETACH` and
re-`ATTACH` it with a higher, **connection-scoped** timeout (via libpq's `options=-c
idle_in_transaction_session_timeout=...`) — never `ALTER DATABASE`, so it never touches the
database-wide default and needs no special Postgres privileges beyond what the catalog's own
`ATTACH` already requires. Detection is deliberately conservative (message-substring matching over
the failure's full cause chain); a failure with no Postgres-specific text anywhere in it will not
trigger escalation. Escalation state is in-memory only and resets on restart, since nothing durable
changes in Postgres.

`postgres_metadata` is a list, one entry per catalog that needs escalation:

```hocon
postgres_metadata = [
  {
    database = "mylake"                 # must match an entry in `databases`
    connection_string = "host=... port=5432 dbname=... user=... password=..."  # libpq key=value,
                                                                                 # no `options=` key
                                                                                 # and no `postgres:`
                                                                                 # prefix (added
                                                                                 # automatically)
    attach_options = "(DATA_PATH 's3://bucket/data')"                          # verbatim clause
                                                                                 # after `AS mylake`
                                                                                 # — must NOT include
                                                                                 # METADATA_PATH
                                                                                 # ':memory:' (below)
  }
]
```

This lives only in `application.conf`/`--conf`, never the `config_provider` table — like
`databases`, it's connection identity needed to reach the database, not a tunable to adjust at
runtime.

**The catalog's own startup-script `ATTACH` must use the `ducklake:postgres:...` DSN form**, not
the bare `ducklake:host=...` form. Verified empirically: without the `postgres:` sub-scheme,
DuckLake silently falls back to a local file catalog named after the literal connection string
instead of storing metadata in Postgres at all (zero `ducklake_*` tables ever appear in Postgres,
and a stray file named after the connection string appears in the working directory). A catalog
attached the bare way has no real Postgres metadata for escalation to reconnect to — re-ATTACHing
it here would silently produce a disconnected, empty catalog instead of raising the timeout on the
real one.

**It must also not use `METADATA_PATH ':memory:'`** — also verified incompatible with a
same-*process* DETACH/re-ATTACH, which is exactly what escalation does: with it, the re-ATTACH
loses visibility into the catalog's own tables; the default local metadata cache re-hydrates from
Postgres correctly. Example of a correctly-shaped ATTACH for an escalation-enabled catalog:

```sql
ATTACH 'ducklake:postgres:host=... dbname=... user=... password=...' AS mylake (DATA_PATH '...');
```

### Startup Script

Configure under `startup_script_provider`:

```hocon
dazzleduck_sql_compaction.startup_script_provider {
  script_location = "/config/startup.sql"
}
```

Or pass inline content:

```hocon
dazzleduck_sql_compaction.startup_script_provider {
  content = "ATTACH 'ducklake:...' AS mydb (DATA_PATH 's3://...');"
}
```

The startup script runs before configuration overrides are read, so catalogs referenced by the
config-provider table must be attached here.

### Configuration Overrides from a Table

Settings can be overlaid from a two-column key/value table (readable after the startup script
has attached its catalog):

```hocon
dazzleduck_sql_compaction.config_provider {
  table = "mydb.main.compactor_config"
  # key_column   = "config_key"   (default)
  # value_column = "value"        (default)
  # prefix       = ""             (optional key prefix filter)
}
```

A configured table that cannot be read is a fatal startup error by design — silently falling
back to file defaults would hide a broken override source.

## Health Check

`GET /health` on `health_port` (default 8080) returns uptime plus per-database compaction
counters (total minor/major compactions, files compacted, last/next execution time, current
small/medium/total file counts). Note: the status is always `UP` while the process is running —
it does not reflect failing compaction cycles.

## Build

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home

# Build fat JAR
./mvnw clean package -pl dazzleduck-sql-ducklake-compactor

# Run tests
./mvnw test -pl dazzleduck-sql-ducklake-compactor

# Build Docker image (Jib, no daemon required)
./mvnw jib:dockerBuild -pl dazzleduck-sql-ducklake-compactor
```

## Running

```bash
java -jar target/dazzleduck-sql-ducklake-compactor-*.jar \
  --conf 'dazzleduck_sql_compaction.databases=[mydb]' \
  --conf 'dazzleduck_sql_compaction.startup_script_provider.script_location=/config/startup.sql'
```

## Docker

```bash
docker run \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  -v /config:/config \
  dazzleduck/ducklake-compactor:latest \
  --conf 'dazzleduck_sql_compaction.databases=[mydb]' \
  --conf 'dazzleduck_sql_compaction.startup_script_provider.script_location=/config/startup.sql'
```

## Metrics

Micrometer metrics are emitted via the logging registry by default:

| Metric | Tags | Description |
|--------|------|-------------|
| `ducklake.compaction.duration` | `type` (minor/major/housekeeping), `step` (merge/expire/cleanup), `database` | Time per compaction step |
| `ducklake.compaction.minor` | `database` | Total minor compactions run |
| `ducklake.compaction.major` | `database` | Total major compactions run |
| `ducklake.files.compacted` | `database` | Total files compacted |
| `ducklake.files.total` | `database` | Total active Parquet files |
| `ducklake.files.small` | `database` | Files below `minor_compaction_max_size` |
| `ducklake.files.medium` | `database` | Files between minor and major thresholds |
