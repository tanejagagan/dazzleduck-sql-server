# DazzleDuck Logback

A Logback appender that forwards application logs to DazzleDuck server using Apache Arrow format.

## Overview

This library provides a custom Logback appender that:
- Captures log events via standard SLF4J/Logback logging
- Buffers logs in-memory with configurable size limits
- Serializes logs to Apache Arrow format for efficient transport
- Forwards logs to DazzleDuck HTTP server
- Supports batching, retries, and disk buffering for reliability

## Requirements

- Java 11+
- Logback

## Installation

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.dazzleduck.sql</groupId>
    <artifactId>dazzleduck-sql-logback</artifactId>
</dependency>
```

## Quick Start

Configure in `logback.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <appender name="DAZZLEDUCK" class="io.dazzleduck.sql.logback.LogForwardingAppender">
        <baseUrl>http://localhost:8081</baseUrl>
        <username>admin</username>
        <password>admin</password>
        <ingestionQueue>log</ingestionQueue>
        <maxBufferSize>10000</maxBufferSize>
    </appender>

    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
        <appender-ref ref="DAZZLEDUCK"/>
    </root>
</configuration>
```

That's it. Logs are automatically forwarded to DazzleDuck.

## Environment Variables

Configuration can be overridden via environment variables:

```xml
<appender name="DAZZLEDUCK" class="io.dazzleduck.sql.logback.LogForwardingAppender">
    <baseUrl>${DAZZLEDUCK_BASE_URL:-http://localhost:8081}</baseUrl>
    <username>${DAZZLEDUCK_USERNAME:-admin}</username>
    <password>${DAZZLEDUCK_PASSWORD:-admin}</password>
    <ingestionQueue>${DAZZLEDUCK_INGESTION_QUEUE:-log}</ingestionQueue>
</appender>
```

## Programmatic Configuration

```java
import io.dazzleduck.sql.logback.LogForwarder;
import io.dazzleduck.sql.logback.LogForwarderConfig;

LogForwarderConfig config = LogForwarderConfig.builder()
        .baseUrl("http://localhost:8081")
        .username("admin")
        .password("admin")
        .ingestionQueue("log")
        .build();

// LogForwarder auto-starts on construction
LogForwarder forwarder = new LogForwarder(config);

// Cleanup on shutdown
Runtime.getRuntime().addShutdownHook(new Thread(forwarder::close));
```

## Configuration Options

| Property | Description | Default |
|----------|-------------|---------|
| `baseUrl` | DazzleDuck server URL | `http://localhost:8081` |
| `username` | Authentication username | `admin` |
| `password` | Authentication password | `admin` |
| `ingestionQueue` | Target ingestion queue | `log` |
| `maxBufferSize` | Max log entries to buffer | `10000` |
| `pollInterval` | How often to send logs | `5 seconds` |
| `minBatchSize` | Min batch size before sending | `1 KB` |
| `maxBatchSize` | Max batch size per request | `10 MB` |
| `maxSendInterval` | Max time between sends | `2 seconds` |
| `enabled` | Enable/disable forwarding | `true` |

## Log Schema

Logs are stored with the following Arrow schema:

| Column | Type | Description |
|--------|------|-------------|
| `s_no` | Long | Sequence number |
| `timestamp` | Timestamp | Timestamp (millisecond precision) |
| `level` | String | Log level (TRACE, DEBUG, INFO, WARN, ERROR) |
| `logger` | String | Logger name |
| `thread` | String | Thread name |
| `message` | String | Formatted log message |
| `mdc` | Map | MDC context values |
| `marker` | String | Log marker (if any) |

## Structured Logging with MDC

```java
import org.slf4j.MDC;

MDC.put("requestId", "REQ-123");
MDC.put("userId", "user-456");
try {
    logger.info("Processing request");
} finally {
    MDC.clear();
}
```

MDC values are captured in the `mdc` column as a map.

## Excluding Packages

To prevent infinite loops, logs from these packages are excluded:
- `io.dazzleduck.sql.logback`
- `io.dazzleduck.sql.client`
- `org.apache.arrow`

## Disabling Forwarding

At runtime:
```java
LogForwardingAppender.setEnabled(false);
```

Or configure with `enabled=false` in logback.xml or programmatically.

## License

Apache License 2.0
