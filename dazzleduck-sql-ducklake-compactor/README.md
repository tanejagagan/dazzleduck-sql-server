# DazzleDuck DuckLake Compaction Service

A background service that runs minor and major compaction on DuckLake catalogs to keep Parquet file counts manageable and reclaim storage from expired snapshots.

## Overview

DuckLake writes small Parquet files on each insert/update. Without compaction, query performance degrades as the file count grows. This service runs two compaction strategies on a schedule:

- **Minor compaction** — merges adjacent small files using `ducklake_merge_adjacent_files`, restricted to files below `minor_compaction_max_size`. Runs frequently (default: every 1 minute).
- **Major compaction** — merges files in the range `[minor_compaction_max_size, major_compaction_max_size)`, expires old snapshots, and cleans up deleted files. Runs less frequently (default: every 1 hour).

Minor and major run on **independent schedules** and, when both enabled, run **concurrently** with
no locking between them — safe only because DuckLake's `min_file_size`/`max_file_size` parameters
fence them to disjoint file-size ranges (enforced at startup: `major_compaction_max_size` must
exceed `minor_compaction_max_size` when both are enabled). Each can be disabled independently via
`minor_compaction.enabled`/`major_compaction.enabled`, and each can run on its own DuckDB connection
settings (e.g. different `memory_limit`/`threads`) via `minor_compaction.connection_settings`/
`major_compaction.connection_settings`.

Both are scheduled at a **fixed rate**, not a fixed delay: a cycle that takes time `T` waits
`frequency - T` before the next one starts, rather than a full `frequency` after completion
regardless of `T`. A cycle that runs longer than its frequency has no negative wait — the next one
starts immediately.

## Configuration

All settings live under the `dazzleduck_sql_compaction` HOCON root in `application.conf` and can be overridden at runtime with `--conf key=value`.

| Key | Default | Description |
|-----|---------|-------------|
| `databases` | `[]` | DuckLake catalog names to compact (must be attached via startup script) |
| `minor_compaction_frequency` | `1 minute` | How often to run minor compaction |
| `major_compaction_frequency` | `1 hour` | How often to run major compaction |
| `minor_compaction_max_size` | `8MB` | Only merge files smaller than this |
| `minor_compaction_max_files` | `1000` | Caps files merged per minor-compaction call (`0` = unbounded); bounds memory/duration of a single cycle on a large catalog, remainder picks up next cycle |
| `major_compaction_max_size` | `64MB` | Only compact files in `[minor_compaction_max_size, major_compaction_max_size)` during major pass |
| `major_compaction_max_files` | `1000` | Same as `minor_compaction_max_files`, for major's call |
| `minor_compaction.enabled` | `true` | Turn minor compaction off entirely for all databases |
| `minor_compaction.connection_settings` | `[]` | Raw SQL run on minor's connection right after opening it, e.g. `["SET memory_limit='2GB'"]` |
| `major_compaction.enabled` | `true` | Turn major compaction off entirely for all databases |
| `major_compaction.connection_settings` | `[]` | Same, for major's connection — also used for that catalog's housekeeping, which shares major's connection rather than getting its own settings |
| `housekeeping_frequency` | `5 minutes` | How often to expire snapshots and delete orphaned files |
| `snapshot_retention` | `15 minutes` | Expire snapshots older than this during housekeeping |
| `health_port` | `8080` | Port for the `GET /health` endpoint |
| `startup_script_provider` | — | How to load the startup SQL (attach catalogs, load extensions) |
| `config_provider` | — | Optional: overlay config values read from a table (see below) |

The bundled defaults live in this module's `application.conf` (not `reference.conf`).

```hocon
minor_compaction {
  enabled = true
  connection_settings = ["SET memory_limit='2GB'", "SET threads=2"]
}
major_compaction {
  enabled = true
  connection_settings = ["SET memory_limit='8GB'", "SET threads=4"]
}
```

Disabling one is a legitimate way to run only the other (e.g. `major_compaction.enabled = false` to
run only minor). Simply raising `minor_compaction_frequency` to a very large value is **not** an
equivalent way to disable minor — with the old single-schedule design that also starved major of any
chance to run; with independent schedules that's no longer true, but the enabled flags are still the
direct way to express "don't run this."

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
