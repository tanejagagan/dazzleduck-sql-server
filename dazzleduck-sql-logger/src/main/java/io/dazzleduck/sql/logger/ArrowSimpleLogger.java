package io.dazzleduck.sql.logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.client.HttpSender;
import io.dazzleduck.sql.common.ingestion.FlightSender;
import io.dazzleduck.sql.common.types.JavaRow;
import org.apache.arrow.vector.types.pojo.*;
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

public class ArrowSimpleLogger extends LegacyAbstractLogger implements AutoCloseable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final Schema schema = new Schema(java.util.List.of(
            new Field("timestamp", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("level", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("logger", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("thread", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("message", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("applicationId", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("applicationName", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("host", FieldType.nullable(new ArrowType.Utf8()), null)
    ));

    private static final Config config = ConfigFactory.load().getConfig("dazzleduck_logger");
    private static final String CONFIG_APPLICATION_ID = config.getString("application_id");
    private static final String CONFIG_APPLICATION_NAME = config.getString("application_name");
    private static final String CONFIG_HOST = config.getString("host");

    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ISO_INSTANT;

    private final String name;
    private final FlightSender flightSender;
    private final String applicationId;
    private final String applicationName;
    private final String host;

    public ArrowSimpleLogger(String name) {
        this(name, createSenderFromConfig());
    }

    public ArrowSimpleLogger(String name, FlightSender sender) {
        this.name = name;
        this.flightSender = sender;
        this.applicationId = CONFIG_APPLICATION_ID;
        this.applicationName = CONFIG_APPLICATION_NAME;
        this.host = CONFIG_HOST;
    }

    private static FlightSender createSenderFromConfig() {
        Config http = config.getConfig("http");

        return new HttpSender(
                schema,
                http.getString("base_url"),
                http.getString("username"),
                http.getString("password"),
                http.getString("target_path"),
                Duration.ofMillis(http.getLong("timeout_ms")),
                config.getLong("min_batch_size"),
                Duration.ofMillis(config.getLong("max_send_interval_ms")),
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

        String message = format(messagePattern, args);
        if (throwable != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            throwable.printStackTrace(pw);
            message += "\n" + sw.toString();
        }
        if (marker != null) {
            message = "[Marker:" + marker.getName() + "] " + message;
        }
        writeArrowAsync(level, message);
    }
    private String format(String pattern, Object[] args) {
        if (args == null || args.length == 0)
            return pattern;
        return MessageFormatter.arrayFormat(pattern, args).getMessage();
    }
    /** Collect logs in batches of 10 and send to Flight */
    private void writeArrowAsync(Level level, String message) {
        try {
            JavaRow row = new JavaRow(new Object[]{
                    TS_FORMAT.format(Instant.now()),
                    level.toString(),
                    name,
                    Thread.currentThread().getName(),
                    message,
                    applicationId,
                    applicationName,
                    host
            });

            // FlightSender handles batching, serialization, and sending
            flightSender.addRow(row);

        } catch (Exception e) {
            System.err.println("[ArrowSimpleLogger] Failed to log:");
            e.printStackTrace(System.err);
        }
    }

    public void close() {
        if (flightSender instanceof FlightSender.AbstractFlightSender afs) {
            afs.close();
        }
    }

    public void log(LoggingEvent event) {
        if (isLevelEnabled(event.getLevel().toInt())) {
            writeArrowAsync(event.getLevel(), event.getMessage());
        }
    }

    // === Log level controls ===
    @Override public boolean isTraceEnabled() { return isLevelEnabled(0); }
    @Override public boolean isDebugEnabled() { return isLevelEnabled(10); }
    @Override public boolean isInfoEnabled()  { return isLevelEnabled(20); }
    @Override public boolean isWarnEnabled()  { return isLevelEnabled(30); }
    @Override public boolean isErrorEnabled() { return isLevelEnabled(40); }

    protected boolean isLevelEnabled(int levelInt) {
        return true; // let backend decide
    }
}