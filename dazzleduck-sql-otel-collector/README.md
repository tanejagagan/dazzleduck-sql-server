# dazzleduck-sql-otel-collector

An OTLP gRPC collector that receives OpenTelemetry signals (logs, traces, metrics) and writes them to Parquet files using Apache Arrow, with optional DuckLake catalog integration.

## Overview

```
OTLP Exporter
     │  gRPC (port 4317)
     ▼
OtelCollectorServer
     │
     ├── OtelLogService      → SignalWriter → ParquetIngestionQueue → Parquet files
     ├── OtelTraceService    → SignalWriter → ParquetIngestionQueue → Parquet files
     └── OtelMetricsService  → SignalWriter → ParquetIngestionQueue → Parquet files
```

Each incoming export request is:
1. Flattened from the 3-level OTLP hierarchy (Resource → Scope → Record) into a flat Arrow batch
2. Written to a temp Arrow file
3. Handed to `SignalWriter`, which batches by size (`min_bucket_size`) and time (`max_delay_ms`) before flushing to Parquet

## Features

- **Three signal types**: logs, traces, metrics — each with a fully typed Arrow schema
- **JWT authentication**: Bearer token validation; supports local user map or login delegation to an external HTTP service
- **Batched writes**: Size + time-based flushing to minimize small Parquet files
- **DuckLake integration**: Optional post-ingestion task registration via `IngestionTaskFactory`
- **SQL transformations**: Derive columns (e.g. partition keys) before writing
- **Micrometer metrics**: Export counters, latency timers, and writer queue gauges

## Configuration

Configuration is loaded from HOCON (`application.conf`) with environment variable and system property overrides.

```hocon
otel_collector {
    grpc_port = 4317

    logs_output_path    = "./otel-logs"
    traces_output_path  = "./otel-traces"
    metrics_output_path = "./otel-metrics"

    ingestion {
        min_bucket_size = 1048576   # 1 MB — flush when accumulated batch size exceeds this
        max_delay_ms    = 5000      # flush after this many ms even if min_bucket_size not reached
    }

    # Columns to partition Parquet output by
    partition_by = []

    # Optional SQL expressions appended as: SELECT *, <transformations>
    # Example: transformations = "cast(timestamp as date) as date"

    # Authentication (required)
    authentication = "jwt"
    secret_key     = "<base64-encoded-hmac-key>"

    # Local users (Basic auth → JWT)
    users = [
        { username = admin, password = admin }
    ]

    # Optional: delegate Basic auth to an external login service
    # login_url = "http://localhost:8081/v1/login"

    # JWT expiration (default 1h)
    # jwt_token.expiration = 1h

    # Service name reported in metrics (default: "open-telemetry-collector")
    # service_name = "my-collector"
}
```

### Environment Variables

Any `otel_collector.*` environment variable overrides the corresponding config key:

```bash
otel_collector.grpc_port=4317
otel_collector.secret_key=<base64-key>
otel_collector.logs_output_path=/data/logs
otel_collector.service_name=my-collector
```

## Authentication

All gRPC calls must be authenticated.

**Step 1 — Login (Basic auth):**

```bash
# Returns a JWT in the Authorization response header
grpcurl -plaintext \
  -H "Authorization: Basic $(echo -n 'admin:admin' | base64)" \
  localhost:4317 opentelemetry.proto.collector.logs.v1.LogsService/Export
```

**Step 2 — Use the token:**

```bash
-H "Authorization: Bearer <jwt-token>"
```

**Login delegation:** Set `login_url` to forward Basic auth credentials to an external HTTP service (same pattern as the DazzleDuck Flight SQL server).

## Arrow Schemas

All schemas flatten the OTLP 3-level hierarchy (Resource → Scope → Record). Resource and scope fields are promoted to top-level columns.

### Logs

| Column | Type | Notes |
|--------|------|-------|
| `timestamp` | Timestamp(ms) | |
| `observed_timestamp` | Timestamp(ms) | |
| `severity_number` | Int32 | |
| `severity_text` | Utf8 | |
| `body` | Utf8 | |
| `trace_id` | Utf8 | hex-encoded |
| `span_id` | Utf8 | hex-encoded |
| `flags` | Int32 | |
| `event_name` | Utf8 | |
| `attributes` | Map(Utf8, Utf8) | log record attributes |
| `resource_attributes` | Map(Utf8, Utf8) | resource attributes |
| `scope_name` | Utf8 | |
| `scope_version` | Utf8 | |

### Traces

| Column | Type | Notes |
|--------|------|-------|
| `trace_id` | Utf8 | |
| `span_id` | Utf8 | |
| `parent_span_id` | Utf8 | |
| `name` | Utf8 | |
| `kind` | Utf8 | INTERNAL / SERVER / CLIENT / PRODUCER / CONSUMER |
| `start_time_ms` | Timestamp(ms) | |
| `end_time_ms` | Timestamp(ms) | |
| `duration_ms` | Int64 | |
| `status_code` | Utf8 | |
| `status_message` | Utf8 | |
| `attributes` | Map(Utf8, Utf8) | |
| `resource_attributes` | Map(Utf8, Utf8) | |
| `scope_name` | Utf8 | |
| `scope_version` | Utf8 | |
| `events` | List(Struct(name, time_ms, attributes)) | |
| `links` | List(Struct(trace_id, span_id, attributes)) | |

### Metrics

Wide-table design — all metric types (GAUGE, SUM, HISTOGRAM, EXPONENTIAL_HISTOGRAM, SUMMARY) share one schema. Columns not applicable to a metric type are null.

| Column | Type | Notes |
|--------|------|-------|
| `name` | Utf8 | |
| `description` | Utf8 | |
| `unit` | Utf8 | |
| `metric_type` | Utf8 | GAUGE / SUM / HISTOGRAM / etc. |
| `start_time_ms` | Timestamp(ms) | |
| `time_ms` | Timestamp(ms) | |
| `attributes` | Map(Utf8, Utf8) | |
| `resource_attributes` | Map(Utf8, Utf8) | |
| `scope_name` | Utf8 | |
| `scope_version` | Utf8 | |
| `value_double` | Double | GAUGE / SUM scalar |
| `value_int` | Int64 | GAUGE / SUM integer |
| `count` | Int64 | HISTOGRAM |
| `sum` | Double | HISTOGRAM |
| `bucket_counts` | List(Int64) | HISTOGRAM |
| `explicit_bounds` | List(Double) | HISTOGRAM |
| `quantile_values` | List(Struct(quantile, value)) | SUMMARY |
| `is_monotonic` | Bool | SUM |
| `aggregation_temporality` | Utf8 | DELTA / CUMULATIVE |

## Micrometer Metrics

`OtelCollectorMetrics` publishes the following meters. When no `MeterRegistry` is configured, a `SimpleMeterRegistry` is used (metrics are active but not published externally).

### Common Tags (on every meter)

| Tag | Source |
|-----|--------|
| `service.name` | `otel_collector.service_name` config (default `"open-telemetry-collector"`) |
| `host.name` | `HOSTNAME` env var → `InetAddress.getLocalHost()` → `"unknown"` |
| `container.id` | `CONTAINER_ID` env var → `"unknown"` |

### Export Metrics

| Metric | Type | Tags |
|--------|------|------|
| `dazzleduck.otel.export.requests` | FunctionCounter | `signal=logs\|traces\|metrics` |
| `dazzleduck.otel.export.records` | FunctionCounter | `signal=logs\|traces\|metrics` |
| `dazzleduck.otel.export.errors` | FunctionCounter | `signal=logs\|traces\|metrics` |
| `dazzleduck.otel.export.latency` | Timer (p50/p95/p99) | `signal=..., max.delay.ms=<configured>` |

`export.latency` covers the full RPC duration — from receiving the request to `onCompleted` or `onError`, including Arrow serialization and `SignalWriter` queue submission.

### Writer Metrics

| Metric | Type | Tags |
|--------|------|------|
| `dazzleduck.otel.writer.bytes_written` | FunctionCounter | `signal=...` |
| `dazzleduck.otel.writer.batches_written` | FunctionCounter | `signal=...` |
| `dazzleduck.otel.writer.pending_batches` | Gauge | `signal=...` |
| `dazzleduck.otel.writer.pending_buckets` | Gauge | `signal=...` |

### Registering a Real Registry

```java
CollectorProperties props = new CollectorConfig().toProperties();
props.setMeterRegistry(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
OtelCollectorServer server = new OtelCollectorServer(props);
server.start();
```

## DuckLake Integration

To register written Parquet files into a DuckLake catalog, configure an `IngestionTaskFactory` per signal:

```java
CollectorProperties props = new CollectorConfig().toProperties();
props.setLogIngestionTaskFactory(myDuckLakeFactory);
props.setTraceIngestionTaskFactory(myDuckLakeFactory);
props.setMetricIngestionTaskFactory(myDuckLakeFactory);
```

Or via HOCON using `ConfigBasedProvider`:

```hocon
otel_collector {
    log_ingestion_task_factory_provider {
        class = "io.dazzleduck.sql.commons.ingestion.DuckLakeIngestionTaskFactoryProvider"
        catalog = "my_catalog"
        mappings = [
            { queue = "logs",    table = "my_catalog.logs" }
            { queue = "traces",  table = "my_catalog.spans" }
            { queue = "metrics", table = "my_catalog.metrics" }
        ]
    }
}
```

## Building

```bash
./mvnw clean package -pl dazzleduck-sql-otel-collector -am -DskipTests
```

## Running

```bash
java -cp target/dazzleduck-sql-otel-collector-*.jar \
     io.dazzleduck.sql.otel.collector.Main
```

Or with an external config file:

```bash
java -cp ... io.dazzleduck.sql.otel.collector.Main -c /etc/otel-collector.conf
```
