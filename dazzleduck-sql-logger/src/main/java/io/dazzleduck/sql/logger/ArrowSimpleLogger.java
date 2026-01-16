package io.dazzleduck.sql.logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.client.HttpArrowProducer;
import io.dazzleduck.sql.client.ArrowProducer;
import io.dazzleduck.sql.common.util.ConfigUtils;
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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ArrowSimpleLogger extends LegacyAbstractLogger implements AutoCloseable {

    @Serial
    private static final long serialVersionUID = 1L;

    // Global sequence number generator
    private static final AtomicLong SEQUENCE_GENERATOR = new AtomicLong(0);

    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ISO_INSTANT;

    // Configuration - initialized FIRST before any Arrow classes to avoid circular initialization
    private static final Config config;
    private static final Level CONFIG_LOG_LEVEL;

    // Flag to track if we're in the middle of static initialization (including schema creation)
    private static final boolean fullyInitialized;

    // Thread-local flag to prevent recursion when creating HttpArrowProducer (which uses SLF4J)
    private static final ThreadLocal<Boolean> creatingProducer = ThreadLocal.withInitial(() -> Boolean.FALSE);

    // Schema - must be declared before static block but initialized lazily
    private static final Schema schema;

    static {
        Config tempConfig = null;
        Level logLevel = Level.INFO;

        try {
            tempConfig = ConfigFactory.load().getConfig("dazzleduck_logger");

            String levelStr = tempConfig.hasPath("log_level") ? tempConfig.getString("log_level") : "INFO";
            logLevel = Level.valueOf(levelStr.toUpperCase());
        } catch (Exception e) {
            System.err.println("[ArrowSimpleLogger] Failed to load config, using defaults: " + e.getMessage());
        }

        config = tempConfig;
        CONFIG_LOG_LEVEL = logLevel;

        // Create schema - this may trigger SLF4J logging from Arrow's Field class
        // Any loggers created during this will use NoOpArrowProducer
        schema = createSchema();

        // Now mark as fully initialized - subsequent loggers can use HttpArrowProducer
        fullyInitialized = true;
    }

    private static Schema createSchema() {
        // MDC field as Map<String, String>
        Field mdcField = new Field("mdc", FieldType.nullable(new ArrowType.Map(false)),
                java.util.List.of(
                        new Field("entries", FieldType.notNullable(new ArrowType.Struct()),
                                java.util.List.of(
                                        new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                                        new Field("value", FieldType.nullable(new ArrowType.Utf8()), null)
                                )
                        )
                )
        );

        return new Schema(java.util.List.of(
                new Field("s_no", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("timestamp", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("level", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("logger", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("thread", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("message", FieldType.nullable(new ArrowType.Utf8()), null),
                mdcField,
                new Field("marker", FieldType.nullable(new ArrowType.Utf8()), null)
        ));
    }

    private final String name;
    private final ArrowProducer flightProducer;

    public ArrowSimpleLogger(String name) {
        this(name, createSenderFromConfig());
    }

    public ArrowSimpleLogger(String name, ArrowProducer sender) {
        this.name = name;
        this.flightProducer = sender;
    }

    private static ArrowProducer createSenderFromConfig() {
        // If not fully initialized, config is unavailable, or we're already creating a producer,
        // return null (no-op). This handles:
        // 1. Circular initialization when Arrow's Field class triggers SLF4J logging
        // 2. Recursive calls when HttpArrowProducer's static initializer uses SLF4J logging
        if (!fullyInitialized || config == null || creatingProducer.get()) {
            return null;
        }

        // Set flag to prevent recursion
        creatingProducer.set(Boolean.TRUE);
        try {
            Config http = config.getConfig(ConfigUtils.HTTP_PREFIX);
            String targetPath = http.getString(ConfigUtils.TARGET_PATH_KEY);
            return new HttpArrowProducer(
                    schema,
                    http.getString(ConfigUtils.BASE_URL_KEY),
                    http.getString(ConfigUtils.USERNAME_KEY),
                    http.getString(ConfigUtils.PASSWORD_KEY),
                    targetPath,
                    Duration.ofMillis(http.getLong(ConfigUtils.HTTP_CLIENT_TIMEOUT_MS_KEY)),
                    config.getLong(ConfigUtils.MIN_BATCH_SIZE_KEY),
                    config.getLong(ConfigUtils.MAX_BATCH_SIZE_KEY),
                    Duration.ofMillis(config.getLong(ConfigUtils.MAX_SEND_INTERVAL_MS_KEY)),
                    config.getInt(ConfigUtils.RETRY_COUNT_KEY),
                    config.getLong(ConfigUtils.RETRY_INTERVAL_MS_KEY),
                    config.getStringList(ConfigUtils.PROJECTIONS_KEY),
                    config.getStringList(ConfigUtils.PARTITION_BY_KEY),
                    config.getLong(ConfigUtils.MAX_IN_MEMORY_BYTES_KEY),
                    config.getLong(ConfigUtils.MAX_ON_DISK_BYTES_KEY)
            );
        } catch (Exception ex) {
            System.err.println("[ArrowSimpleLogger] Failed to create HttpArrowProducer: " + ex.getMessage());
            return null;
        } finally {
            creatingProducer.set(Boolean.FALSE);
        }
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
        if (flightProducer == null) {
            return;
        }

        try {
            long sequenceNo = SEQUENCE_GENERATOR.incrementAndGet();
            String timestamp = TS_FORMAT.format(Instant.now());
            String threadName = Thread.currentThread().getName();

            // Get MDC context - use empty map if null
            Map<String, String> mdcMap = MDC.getCopyOfContextMap();
            if (mdcMap == null) {
                mdcMap = Collections.emptyMap();
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
                    mdcMap,
                    markerName
            });

            // ArrowProducer handles batching, serialization, and sending
            flightProducer.addRow(row);

        } catch (Exception e) {
            System.err.println("[ArrowSimpleLogger] Failed to log:");
            e.printStackTrace(System.err);
        }
    }

    public void close() {
        if (flightProducer instanceof ArrowProducer.AbstractArrowProducer afs) {
            afs.close();
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

    /**
     * Get the Arrow schema used for log records.
     */
    public static Schema getSchema() {
        return schema;
    }
}