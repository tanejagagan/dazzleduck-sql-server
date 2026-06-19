# Dynamic Ingestion Queue Management

The otel-collector supports a **dynamic queue registry** backed by a SQLite database.
Queues can be added, updated, or deleted at runtime by any process that writes to the SQLite file —
no collector restart is required.

## How It Works

```
External process              SQLite file                  otel-collector
──────────────────      ──────────────────────────      ────────────────────────────
INSERT / UPDATE /   →   ingestion_queues table      →   background thread detects
DELETE row              mtime bumped on every write      mtime change, reloads registry
                                                         within config_load_interval_ms
```

1. The collector opens the SQLite file read-only via DuckDB's SQLite extension at startup.
2. A background thread polls the file's **last-modified time** every `config_load_interval_ms`
   milliseconds (default 5 000 ms).
3. When the mtime changes the thread re-reads the entire `ingestion_queues` table and applies
   the diff to the in-memory registry.
4. The hot path (every incoming OTLP RPC) reads only from the in-memory registry — no SQLite
   I/O on the critical path.

## Enabling the Dynamic Provider

In your HOCON config, set the provider class and point it at the SQLite file:

```hocon
otel_collector {
    ingestion_task_factory_provider {
        class                   = "io.dazzleduck.sql.commons.ingestion.DynamicDuckLakeIngestionTaskFactoryProvider"
        db_path                 = "/var/data/ingestion-queues.db"
        config_load_interval_ms = 5000   # optional, default 5000
    }
}
```

The SQLite file is created and the schema is initialised automatically on first startup
if the file does not exist.

## SQLite Schema

```sql
CREATE TABLE ingestion_queues (
    ingestion_queue  TEXT PRIMARY KEY,  -- queue ID used in JWT x-dd-ingestion-queue claim
    output_path      TEXT NOT NULL,     -- destination directory for Parquet files
    catalog          TEXT NOT NULL,     -- DuckLake catalog name
    schema_name      TEXT NOT NULL,     -- DuckLake schema (e.g. "main")
    table_name       TEXT NOT NULL,     -- DuckLake table name
    transformation   TEXT,             -- optional SQL expressions added as SELECT *, <expr>
    view_name        TEXT,             -- optional view name (reserved for future use)
    input_table      TEXT,             -- optional source table override
    partition_by     TEXT,             -- reserved; not yet applied at runtime
    min_bucket_size  INTEGER,          -- flush threshold in bytes (overrides global setting)
    max_delay_ms     INTEGER           -- flush interval in ms (overrides global setting)
);
```

## Queue Lifecycle Operations

Any process with write access to the SQLite file can perform the following operations.
Changes are picked up within one `config_load_interval_ms` interval.

### Add a queue

```sql
INSERT INTO ingestion_queues
    (ingestion_queue, output_path, catalog, schema_name, table_name)
VALUES
    ('app-logs', 's3://my-bucket/app-logs/', 'my_catalog', 'main', 'app_logs');
```

Once the collector picks up the change, any OTLP request whose JWT contains
`x-dd-ingestion-queue: app-logs` will start writing to the new queue.
The `ParquetIngestionQueue` is created lazily on the first incoming message.

### Update a queue (e.g. change transformation or output path)

```sql
UPDATE ingestion_queues
SET transformation = 'CAST(timestamp AS DATE) AS date, severity_text AS level'
WHERE ingestion_queue = 'app-logs';
```

The updated `transformation` SQL is applied on the next batch flush — in-flight batches
complete with the old transformation first.

### Delete a queue

```sql
DELETE FROM ingestion_queues WHERE ingestion_queue = 'app-logs';
```

The collector closes the queue (flushing any pending buffered data to Parquet) and stops
accepting new messages for that queue ID. Subsequent requests referencing the deleted queue ID
are rejected with `INVALID_ARGUMENT`.

## Concurrency Notes

- **One writer at a time**: SQLite does not support concurrent writers. If multiple external
  processes need to modify the registry, serialize their writes or use a single coordinator.
- **Collector is read-only**: The collector never writes to the SQLite file — it is always
  opened with `READ_ONLY true`. External writers can update the file at any time without
  coordination with the collector.
- **Atomic visibility**: The collector reloads the full table on each detected change, so a
  batch of inserts/updates committed in a single SQLite transaction is always seen atomically.

## Updating the Registry from DuckDB

DuckDB's SQLite extension can be used as a convenient admin interface:

```sql
-- Attach the registry (read-write)
ATTACH '/var/data/ingestion-queues.db' AS reg (TYPE sqlite);

-- List current queues
SELECT * FROM reg.ingestion_queues;

-- Add a queue
INSERT INTO reg.ingestion_queues
    (ingestion_queue, output_path, catalog, schema_name, table_name)
VALUES ('audit-logs', 's3://my-bucket/audit/', 'my_catalog', 'main', 'audit_logs');

-- Update transformation
UPDATE reg.ingestion_queues
SET transformation = 'CAST(timestamp AS DATE) AS date'
WHERE ingestion_queue = 'audit-logs';

-- Remove a queue
DELETE FROM reg.ingestion_queues WHERE ingestion_queue = 'audit-logs';

DETACH reg;
```

## Updating the Registry from Python

```python
import sqlite3

con = sqlite3.connect("/var/data/ingestion-queues.db")

# Add a queue
con.execute("""
    INSERT INTO ingestion_queues (ingestion_queue, output_path, catalog, schema_name, table_name)
    VALUES (?, ?, ?, ?, ?)
""", ("audit-logs", "s3://my-bucket/audit/", "my_catalog", "main", "audit_logs"))

# Update output path
con.execute("""
    UPDATE ingestion_queues SET output_path = ? WHERE ingestion_queue = ?
""", ("s3://new-bucket/audit/", "audit-logs"))

# Delete a queue
con.execute("DELETE FROM ingestion_queues WHERE ingestion_queue = ?", ("audit-logs",))

con.commit()
con.close()
```

## Limitations

- `partition_by` is stored in the schema but is **not applied at runtime** — the collector
  always uses an empty partition list regardless of this column's value.
- `min_bucket_size` and `max_delay_ms` per-queue overrides are stored but not yet wired
  into the queue creation path — the global values from `ingestion` config are used.
- The reload is triggered by file mtime. On filesystems where writes do not reliably update
  mtime (e.g. some network filesystems), changes may not be detected.
