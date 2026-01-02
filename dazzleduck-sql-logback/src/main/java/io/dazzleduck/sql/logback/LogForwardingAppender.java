package io.dazzleduck.sql.logback;


import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import lombok.Setter;

/**
 * Custom Logback appender that captures log events and buffers them
 * for forwarding to a remote server.
 */
@Setter
public class LogForwardingAppender extends AppenderBase<ILoggingEvent> {

    // Static buffer shared across all instances
    private static volatile LogBuffer buffer;
    private static volatile String application_id;
    private static volatile String application_name;
    private static volatile String application_host;
    /**
     * -- SETTER --
     *  Enable or disable log forwarding.
     */
    @Setter
    private static volatile boolean enabled = true;

    // Setter for Logback XML configuration
    // Logback XML configurable properties
    private int maxBufferSize = 10000;

    @Override
    public void start() {
        if (buffer == null) {
            synchronized (LogForwardingAppender.class) {
                if (buffer == null) {
                    buffer = new LogBuffer(maxBufferSize);
                }
            }
        }
        super.start();
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (!enabled) {
            return;
        }

        // Avoid logging loops - don't capture our own forwarding logs
        String loggerName = event.getLoggerName();
        if (loggerName != null && loggerName.startsWith("com.one211.application.logging")) {
            return;
        }

        LogEntry entry = LogEntry.from(
                event,
                application_id,
                application_name,
                application_host
        );

        if (!buffer.offer(entry)) {
            // Buffer full - log to console only (avoid infinite loop)
            addWarn("Log buffer full, dropping entry: " + event.getFormattedMessage());
        }
    }

    /**
     * Configure the appender with Spring-managed properties.
     */
    public static void configure(String appId, String appName, String appHost, int bufferSize, boolean isEnabled) {
        application_id = appId;
        application_name = appName;
        // Use configured host if provided, otherwise keep auto-detected
        if (appHost != null && !appHost.isEmpty()) {
            application_host = appHost;
        }
        enabled = isEnabled;
        if (buffer == null || buffer.getSize() == 0) {
            buffer = new LogBuffer(bufferSize);
        }
    }

    /**
     * Get the shared buffer instance.
     */
    public static LogBuffer getBuffer() {
        if (buffer == null) {
            synchronized (LogForwardingAppender.class) {
                if (buffer == null) {
                    buffer = new LogBuffer(10000);
                }
            }
        }
        return buffer;
    }
}
