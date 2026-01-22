# Arrow-Logback

A Logback appender that forwards application logs to any HTTP server using Apache Arrow format.

## Overview

This library provides a custom Logback appender that:
- Captures log events from your application via standard SLF4J/Logback logging
- Buffers logs in-memory with configurable size limits
- Serializes logs to Apache Arrow format for efficient transport
- Forwards logs to any HTTP server that accepts Arrow data
- Supports batching, retries, and disk buffering for reliability

## Requirements

- Java 21+
- Maven 3.6+

## Installation

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.dazzleduck.sql</groupId>
    <artifactId>dazzleduck-sql-logback</artifactId>
    <version>0.0.16</version>
</dependency>
```

## Quick Start

### Option 1: Configuration File (Recommended)

Create an `application.conf` file in your resources:

```hocon
dazzleduck_logback = {
  application_id   = "my-app"
  application_name = "My Application"
  application_host = "localhost"
  enabled          = true

  # Buffer settings
  max_buffer_size  = 10000
  poll_interval_ms = 5000

  # Sender settings
  min_batch_size       = 1048576    # 1 MB
  max_batch_size       = 16777216   # 16 MB
  max_send_interval_ms = 1000
  max_in_memory_bytes  = 10485760   # 10 MB
  max_on_disk_bytes    = 1073741824 # 1 GB
  retry_count          = 3
  retry_interval_ms    = 1000
  transformations      = []
  partition_by         = []

  http {
    base_url               = "http://localhost:8081"
    username               = "admin"
    password               = "admin"
    target_path            = "logs"
    http_client_timeout_ms = 5000
  }
}
```

Then use the factory:

```java
import io.dazzleduck.sql.logback.LogForwarder;
import io.dazzleduck.sql.logback.LogForwarderConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingExample {
    private static final Logger logger = LoggerFactory.getLogger(LoggingExample.class);

    public static void main(String[] args) {
        // Create forwarder from application.conf
        LogForwarder forwarder = LogForwarderConfigFactory.createForwarder();

        // Now use standard SLF4J logging - logs are automatically captured
        logger.info("Application started");
        logger.debug("Debug message");
        logger.warn("Warning message");
        logger.error("Error occurred", new RuntimeException("Sample error"));

        // Cleanup on shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(forwarder::close));
    }
}
```

### Option 2: Programmatic Configuration

```java
import io.dazzleduck.sql.logback.LogForwarder;
import io.dazzleduck.sql.logback.LogForwarderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class LoggingExample {
    private static final Logger logger = LoggerFactory.getLogger(LoggingExample.class);

    public static void main(String[] args) {
        // Build configuration
        LogForwarderConfig config = LogForwarderConfig.builder()
                .baseUrl("http://localhost:8081")
                .username("admin")
                .password("admin")
                .targetPath("logs")
                .applicationId("my-app")
                .applicationName("My Application")
                .pollInterval(Duration.ofSeconds(5))
                .build();

        // Create and start the forwarder
        LogForwarder forwarder = LogForwarder.createAndStart(config);

        // Now use standard SLF4J logging - logs are automatically captured
        logger.info("Application started");
        logger.debug("Debug message");
        logger.warn("Warning message");
        logger.error("Error occurred", new RuntimeException("Sample error"));

        // Cleanup on shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(forwarder::close));
    }
}
```

## Usage Examples

### Basic Logging

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyService {
    private static final Logger logger = LoggerFactory.getLogger(MyService.class);

    public void doSomething() {
        logger.info("Processing started");

        try {
            // Business logic
            logger.debug("Step 1 completed");
            logger.debug("Step 2 completed");
            logger.info("Processing completed successfully");
        } catch (Exception e) {
            logger.error("Processing failed", e);
        }
    }
}
```

### Structured Logging with MDC

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class RequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

    public void handleRequest(String requestId, String userId) {
        MDC.put("requestId", requestId);
        MDC.put("userId", userId);

        try {
            logger.info("Request received");
            // Process request
            logger.info("Request completed");
        } finally {
            MDC.clear();
        }
    }
}
```

### Different Log Levels

```java
logger.trace("Very detailed message");  // Most verbose
logger.debug("Debug information");
logger.info("General information");
logger.warn("Warning message");
logger.error("Error message");          // Most severe
```

## Configuration Options

| Property | Description | Default |
|----------|-------------|---------|
| `applicationId` | Unique identifier for your application | `default-app` |
| `applicationName` | Human-readable application name | `DefaultApplication` |
| `applicationHost` | Hostname of the application | auto-detected |
| `baseUrl` | Target server URL | `http://localhost:8081` |
| `username` | Authentication username | `admin` |
| `password` | Authentication password | `admin` |
| `targetPath` | Server endpoint path | `logs` |
| `httpClientTimeout` | HTTP request timeout | `3 seconds` |
| `maxBufferSize` | Maximum number of log entries to buffer | `10000` |
| `pollInterval` | How often to drain buffer and send logs | `5 seconds` |
| `minBatchSize` | Minimum batch size before sending | `1 MB` |
| `maxBatchSize` | Maximum batch size per request | `10 MB` |
| `maxSendInterval` | Maximum time between sends | `2 seconds` |
| `maxInMemorySize` | Max memory buffer size | `10 MB` |
| `maxOnDiskSize` | Max disk buffer size | `1 GB` |
| `retryCount` | Number of retry attempts | `3` |
| `retryIntervalMillis` | Delay between retries | `1000 ms` |
| `transformations` | SQL transformations for logs | `[]` |
| `partitionBy` | Partition columns for storage | `[]` |
| `enabled` | Enable/disable forwarding | `true` |

## Log Schema

Logs are stored with the following Arrow schema:

| Column | Type | Description |
|--------|------|-------------|
| `s_no` | Long | Sequence number |
| `timestamp` | String (ISO-8601) | When the log was created |
| `level` | String | Log level (TRACE, DEBUG, INFO, WARN, ERROR) |
| `logger` | String | Logger name (usually class name) |
| `thread` | String | Thread name |
| `message` | String | Formatted log message |
| `application_id` | String | Application identifier (added via transformation) |
| `application_name` | String | Application name (added via transformation) |
| `application_host` | String | Host where log originated (added via transformation) |

## Spring Boot Integration

```java
import io.dazzleduck.sql.logback.LogForwarder;
import io.dazzleduck.sql.logback.LogForwarderConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;
import java.time.Duration;

@Configuration
public class LoggingConfig {

    private LogForwarder logForwarder;

    @Bean
    public LogForwarder logForwarder() {
        LogForwarderConfig config = LogForwarderConfig.builder()
                .baseUrl("http://log-server:8081")
                .username("admin")
                .password("admin")
                .targetPath("logs")
                .applicationId("spring-app")
                .applicationName("Spring Boot Application")
                .pollInterval(Duration.ofSeconds(5))
                .maxBufferSize(20000)
                .enabled(true)
                .build();

        this.logForwarder = LogForwarder.createAndStart(config);
        return logForwarder;
    }

    @PreDestroy
    public void cleanup() {
        if (logForwarder != null) {
            logForwarder.close();
        }
    }
}
```

## Complete Example: Web Application Logging

```java
import io.dazzleduck.sql.logback.LogForwarder;
import io.dazzleduck.sql.logback.LogForwarderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class WebAppLoggingExample {

    private static final Logger logger = LoggerFactory.getLogger(WebAppLoggingExample.class);
    private final LogForwarder logForwarder;

    public WebAppLoggingExample() {
        // Configure and start the forwarder
        LogForwarderConfig config = LogForwarderConfig.builder()
                .baseUrl("http://localhost:8081")
                .username("admin")
                .password("admin")
                .targetPath("logs")
                .applicationId("webapp-001")
                .applicationName("Web Application")
                .pollInterval(Duration.ofSeconds(5))
                .maxBufferSize(50000)
                .retryCount(5)
                .build();

        this.logForwarder = LogForwarder.createAndStart(config);

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(logForwarder::close));

        logger.info("Web application started");
    }

    public String handleRequest(String path, String method) {
        logger.info("Received {} request for {}", method, path);

        try {
            // Simulate request processing
            Thread.sleep(100);

            logger.debug("Request processing completed for {}", path);
            return "OK";
        } catch (Exception e) {
            logger.error("Failed to process request for {}", path, e);
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        WebAppLoggingExample app = new WebAppLoggingExample();

        // Simulate some requests
        for (int i = 0; i < 100; i++) {
            app.handleRequest("/api/users", "GET");
            app.handleRequest("/api/orders", "POST");
            Thread.sleep(50);
        }

        logger.info("Application shutting down");

        // Wait for logs to be forwarded
        Thread.sleep(10000);
    }
}
```

## Lifecycle Management

```java
LogForwarder forwarder = LogForwarder.createAndStart(config);

// Check status
boolean running = forwarder.isRunning();
int bufferSize = forwarder.getBufferSize();

// Stop forwarding (flushes remaining logs)
forwarder.stop();

// Full cleanup
forwarder.close();
```

## Disabling Log Forwarding

You can disable log forwarding without changing code:

```java
LogForwarderConfig config = LogForwarderConfig.builder()
        // ... other config ...
        .enabled(false)
        .build();
```

Or via configuration file:

```hocon
dazzleduck_logback {
  enabled = false
  // ... rest of config ...
}
```

Or at runtime:

```java
import io.dazzleduck.sql.logback.LogForwardingAppender;

// Disable forwarding
LogForwardingAppender.setEnabled(false);

// Re-enable forwarding
LogForwardingAppender.setEnabled(true);
```

## Excluded Packages

To prevent infinite logging loops, logs from the following packages are automatically excluded:
- `io.dazzleduck.sql.logback`
- `io.dazzleduck.sql.client`
- `org.apache.arrow`

## Troubleshooting

### Logs not appearing on server

1. Check that the server URL is correct and accessible
2. Verify authentication credentials
3. Ensure `enabled = true` in `application.conf`
4. Verify the `LogForwarder` is started before logging begins

### Buffer full warnings

Increase buffer size or reduce poll interval:

```java
.maxBufferSize(50000)           // More buffer capacity
.pollInterval(Duration.ofSeconds(2))  // More frequent forwarding
```

### High memory usage

Reduce buffer sizes:

```java
.maxBufferSize(5000)
.maxInMemorySize(5 * 1024 * 1024)  // 5 MB
```

### Missing logs

Check that your logging framework's level allows the messages. For Logback, you can configure the root level in `logback.conf`:

```hocon
# In logback.conf or programmatically
root.level = DEBUG  # Change from INFO to DEBUG to capture debug messages
```

## License

Apache License 2.0
