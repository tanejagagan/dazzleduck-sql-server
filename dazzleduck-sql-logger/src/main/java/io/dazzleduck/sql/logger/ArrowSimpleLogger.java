package io.dazzleduck.sql.logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.client.HttpProducer;
import io.dazzleduck.sql.client.FlightProducer;
import io.dazzleduck.sql.common.util.ConfigUtils;
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
            new Field("application_id", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("application_name", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("application_host", FieldType.nullable(new ArrowType.Utf8()), null)
    ));

    private static final Config config = ConfigFactory.load().getConfig("dazzleduck_logger");
    private static final String CONFIG_APPLICATION_ID = config.getString(ConfigUtils.APPLICATION_ID_KEY);
    private static final String CONFIG_APPLICATION_NAME = config.getString(ConfigUtils.APPLICATION_NAME_KEY);
    private static final String CONFIG_APPLICATION_HOST = config.getString(ConfigUtils.APPLICATION_HOST_KEY);

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
        Config http = config.getConfig(ConfigUtils.HTTP_PREFIX);
        String targetPath = http.getString(ConfigUtils.TARGET_PATH_KEY);
        return new HttpProducer(
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
                config.getStringList(ConfigUtils.TRANSFORMATIONS_KEY),
                config.getStringList(ConfigUtils.PARTITION_BY_KEY),
                config.getLong(ConfigUtils.MAX_IN_MEMORY_BYTES_KEY),
                config.getLong(ConfigUtils.MAX_ON_DISK_BYTES_KEY)
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
                    application_id,
                    application_name,
                    application_host
            });

            // FlightSender handles batching, serialization, and sending
            flightProducer.addRow(row);

        } catch (Exception e) {
            System.err.println("[ArrowSimpleLogger] Failed to log:");
            e.printStackTrace(System.err);
        }
    }

    public void close() {
        if (flightProducer instanceof FlightProducer.AbstractFlightProducer afs) {
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