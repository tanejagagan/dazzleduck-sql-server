package io.dazzleduck.sql.logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.client.HttpProducer;
import io.dazzleduck.sql.client.FlightProducer;
import io.dazzleduck.sql.common.types.JavaRow;
import org.apache.arrow.vector.types.pojo.*;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;
import org.slf4j.helpers.LegacyAbstractLogger;
import org.slf4j.helpers.MessageFormatter;

import java.io.PrintWriter;
import java.io.Serial;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ArrowSimpleLogger extends LegacyAbstractLogger implements AutoCloseable {

    @Serial
    private static final long serialVersionUID = 1L;

    // Global sequence number generator
    private static final AtomicLong SEQUENCE_GENERATOR = new AtomicLong(0);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static final Schema schema = new Schema(java.util.List.of(
            new Field("s_no", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("timestamp", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("level", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("logger", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("thread", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("message", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("mdc", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("marker", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("application_id", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("application_name", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("application_host", FieldType.nullable(new ArrowType.Utf8()), null)
    ));

    private static final Config config;
    private static final String CONFIG_APPLICATION_ID;
    private static final String CONFIG_APPLICATION_NAME;
    private static final String CONFIG_APPLICATION_HOST;
    private static final Level CONFIG_LOG_LEVEL;

    static {
        config = ConfigFactory.load().getConfig("dazzleduck_logger");
        CONFIG_APPLICATION_ID = config.getString("application_id");
        CONFIG_APPLICATION_NAME = config.getString("application_name");
        CONFIG_APPLICATION_HOST = config.getString("application_host");

        // Read log level from config with default to INFO
        String levelStr = config.hasPath("log_level") ? config.getString("log_level") : "INFO";
        CONFIG_LOG_LEVEL = Level.valueOf(levelStr.toUpperCase());
    }

    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ISO_INSTANT;

    private final String name;
    private final FlightProducer flightProducer;
    private final String application_id;
    private final String application_name;
    private final String application_host;

    public ArrowSimpleLogger(String name) {
        this(name, createSenderFromConfig());
    }

    public ArrowSimpleLogger(String name, FlightProducer sender) {
        this.name = name;
        this.flightProducer = sender;
        this.application_id = CONFIG_APPLICATION_ID;
        this.application_name = CONFIG_APPLICATION_NAME;
        this.application_host = CONFIG_APPLICATION_HOST;
    }

    private static FlightProducer createSenderFromConfig() {
        Config http = config.getConfig("http");
        String targetPath = http.getString("target_path");
        return new HttpProducer(
                schema,
                http.getString("base_url"),
                http.getString("username"),
                http.getString("password"),
                targetPath,
                Duration.ofMillis(http.getLong("http_client_timeout_ms")),
                config.getLong("min_batch_size"),
                config.getLong("max_batch_size"),
                Duration.ofMillis(config.getLong("max_send_interval_ms")),
                config.getInt("retry_count"),
                config.getLong("retry_interval_ms"),
                config.getStringList("transformations"),
                config.getStringList("partition_by"),
                config.getLong("max_in_memory_bytes"),
                config.getLong("max_on_disk_bytes")
        );
    }

    @Override
    protected String getFullyQualifiedCallerName() {
        return name;
    }

    @Override
    protected void handleNormalizedLoggingCall(
            Level level, Marker marker, String messagePattern,
            Object[] args, Throwable throwable) {

        if (!isLevelEnabled(level.toInt())) {
            return;
        }

        String message = format(messagePattern, args);
        if (throwable != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            throwable.printStackTrace(pw);
            message += "\n" + sw.toString();
        }

        writeArrowAsync(level, marker, message);
    }

    private String format(String pattern, Object[] args) {
        if (args == null || args.length == 0)
            return pattern;
        return MessageFormatter.arrayFormat(pattern, args).getMessage();
    }

    /** Collect logs and send to Flight */
    private void writeArrowAsync(Level level, Marker marker, String message) {
        try {
            long sequenceNo = SEQUENCE_GENERATOR.incrementAndGet();
            String timestamp = TS_FORMAT.format(Instant.now());
            String threadName = Thread.currentThread().getName();

            // Get MDC context
            Map<String, String> mdcMap = MDC.getCopyOfContextMap();
            String mdcJson = null;
            if (mdcMap != null && !mdcMap.isEmpty()) {
                try {
                    mdcJson = JSON_MAPPER.writeValueAsString(mdcMap);
                } catch (JsonProcessingException e) {
                    System.err.println("[ArrowSimpleLogger] Failed to serialize MDC: " + e.getMessage());
                }
            }

            // Get marker name
            String markerName = marker != null ? marker.getName() : null;

            JavaRow row = new JavaRow(new Object[]{
                    sequenceNo,
                    timestamp,
                    level.toString(),
                    name,
                    threadName,
                    message,
                    mdcJson,
                    markerName,
                    application_id,
                    application_name,
                    application_host
            });

            // FlightProducer handles batching, serialization, and sending
            flightProducer.addRow(row);

        } catch (Exception e) {
            System.err.println("[ArrowSimpleLogger] Failed to log:");
            e.printStackTrace(System.err);
        }
    }

    @Override
    public void close() {
        try {
            if (flightProducer != null) {
                ((AutoCloseable) flightProducer).close();
            }
        } catch (Exception e) {
            System.err.println("[ArrowSimpleLogger] Failed to close producer:");
            e.printStackTrace(System.err);
        }
    }

    public void log(LoggingEvent event) {
        if (isLevelEnabled(event.getLevel().toInt())) {
            writeArrowAsync(event.getLevel(), event.getMarkers().isEmpty() ? null : event.getMarkers().get(0), event.getMessage());
        }
    }

    // === Log level controls ===
    @Override
    public boolean isTraceEnabled() {
        return isLevelEnabled(Level.TRACE.toInt());
    }

    @Override
    public boolean isDebugEnabled() {
        return isLevelEnabled(Level.DEBUG.toInt());
    }

    @Override
    public boolean isInfoEnabled() {
        return isLevelEnabled(Level.INFO.toInt());
    }

    @Override
    public boolean isWarnEnabled() {
        return isLevelEnabled(Level.WARN.toInt());
    }

    @Override
    public boolean isErrorEnabled() {
        return isLevelEnabled(Level.ERROR.toInt());
    }

    protected boolean isLevelEnabled(int levelInt) {
        return levelInt >= CONFIG_LOG_LEVEL.toInt();
    }
}