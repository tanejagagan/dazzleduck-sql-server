# DazzleDuck SQL Logger

A drop-in SLF4J logging provider that sends logs directly to a DazzleDuck server in Apache Arrow format. Logs are batched, compressed, and stored as Parquet files for high-performance analytics.

## Features

- **SLF4J 2.0 Compatible** - Use standard SLF4J API, logs are automatically sent to DazzleDuck
- **Apache Arrow Format** - Efficient columnar serialization for fast analytics
- **Automatic Batching** - Logs are batched based on size and time for optimal throughput
- **MDC Support** - Full support for Mapped Diagnostic Context
- **Marker Support** - Log markers are captured and stored
- **Fault Tolerant** - Retry logic with disk spillover for backpressure handling

---

## Quick Start

### 1. Add Dependency

```xml
<dependency>
    <groupId>io.dazzleduck.sql</groupId>
    <artifactId>dazzleduck-sql-logger</artifactId>
    <version>0.0.13-SNAPSHOT</version>
</dependency>
```

**Important:** Do not include any other SLF4J provider (like `slf4j-simple`, `logback-classic`, or `log4j-slf4j-impl`) in your runtime classpath. This library IS your SLF4J provider.

### 2. Configure

Create `application.conf` in your classpath (e.g., `src/main/resources/`):

```hocon
dazzleduck_logger {
  # Application identification (appears in every log record)
  application_id   = "my-app-001"
  application_name = "MyApplication"
  application_host = "localhost"

  # Log level (TRACE, DEBUG, INFO, WARN, ERROR)
  log_level = "INFO"

  # HTTP connection to DazzleDuck server
  http {
    base_url              = "http://localhost:8081"
    username              = "admin"
    password              = "admin"
    target_path           = "logs"
    http_client_timeout_ms = 3000
  }

  # Batching configuration
  min_batch_size       = 1048576   # 1 MB - minimum batch before sending
  max_batch_size       = 16777216  # 16 MB - maximum batch size
  max_send_interval_ms = 2000      # Send if no activity for 2 seconds

  # Retry configuration
  retry_count       = 3
  retry_interval_ms = 1000

  # Memory management
  max_in_memory_bytes = 10485760    # 10 MB in memory before spilling to disk
  max_on_disk_bytes   = 1073741824  # 1 GB max disk usage
}
```

### 3. Use Standard SLF4J API

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class MyApplication {
    private static final Logger logger = LoggerFactory.getLogger(MyApplication.class);

    public static void main(String[] args) {
        // Basic logging
        logger.info("Application started");
        logger.debug("Debug message with value: {}", 42);
        logger.warn("Warning: {} items remaining", 5);

        // Logging with MDC context
        MDC.put("request_id", "req-12345");
        MDC.put("user_id", "user-789");
        try {
            logger.info("Processing request");
            // ... your code
        } finally {
            MDC.clear();
        }

        // Exception logging
        try {
            riskyOperation();
        } catch (Exception e) {
            logger.error("Operation failed", e);
        }
    }
}
```

---

## Log Schema

Each log entry is stored with the following Arrow schema:

| Field | Type | Description |
|-------|------|-------------|
| `s_no` | Int64 | Global sequence number |
| `timestamp` | Utf8 | ISO-8601 timestamp |
| `level` | Utf8 | Log level (TRACE, DEBUG, INFO, WARN, ERROR) |
| `logger` | Utf8 | Logger name (usually class name) |
| `thread` | Utf8 | Thread name |
| `message` | Utf8 | Formatted log message |
| `mdc` | Utf8 | MDC context as JSON |
| `marker` | Utf8 | Log marker name |
| `application_id` | Utf8 | Application identifier |
| `application_name` | Utf8 | Application name |
| `application_host` | Utf8 | Host name |

---

## Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `application_id` | - | Unique identifier for your application |
| `application_name` | - | Human-readable application name |
| `application_host` | - | Host/server name |
| `log_level` | `INFO` | Minimum log level to capture |
| `http.base_url` | - | DazzleDuck server URL |
| `http.username` | - | Authentication username |
| `http.password` | - | Authentication password |
| `http.target_path` | - | Target table/path in DazzleDuck |
| `http.http_client_timeout_ms` | `3000` | HTTP request timeout |
| `min_batch_size` | `1048576` | Minimum batch size before sending (bytes) |
| `max_batch_size` | `16777216` | Maximum batch size (bytes) |
| `max_send_interval_ms` | `2000` | Force send after this many ms |
| `retry_count` | `3` | Number of retry attempts |
| `retry_interval_ms` | `1000` | Delay between retries |
| `max_in_memory_bytes` | `10485760` | Max memory before disk spillover |
| `max_on_disk_bytes` | `1073741824` | Max disk usage for spillover |

---

## Advanced: Log File Tailing

This module also supports tailing existing JSON log files and sending them to DazzleDuck. This is useful for ingesting logs from applications that write to files.

### Log File Format

Log files must be JSON-per-line format:

```json
{"timestamp": "2024-01-01T10:00:00Z", "level": "INFO", "logger": "App", "thread": "main", "message": "Hello"}
{"timestamp": "2024-01-01T10:00:01Z", "level": "WARN", "logger": "App", "thread": "main", "message": "Warning"}
```

### Running the Log Processor

```bash
java -cp dazzleduck-sql-logger.jar io.dazzleduck.sql.logger.tailing.LogProcessorMain
```

Configure via `application.conf`:

```hocon
dazzleduck_logger {
  # Directory to monitor
  log_directory    = "/var/logs/"
  log_file_pattern = "*.log"
  poll_interval_ms = 30000  # Check every 30 seconds

  # ... other settings as above
}
```

### Core Components

- **LogFileTailReader** - Monitors directory, tracks file positions, reads new lines
- **LogTailToArrowProcessor** - Orchestrates tailing, conversion, and sending
- **JsonToArrowConverter** - Converts JSON log entries to Arrow format
- **HttpProducer** - Handles HTTP transport with batching, retries, and backpressure

---

## Requirements

- Java 21+
- DazzleDuck SQL Server (HTTP mode with JWT authentication)

---

## Architecture

```
Application Code
       |
       v
  SLF4J API (Logger.info(), etc.)
       |
       v
  ArrowSimpleLogger (this library)
       |
       v
  FlightProducer (batching, serialization)
       |
       v
  HttpProducer (HTTP POST with Arrow binary)
       |
       v
  DazzleDuck Server (ingestion -> Parquet)
```

---

## License

Apache 2.0
