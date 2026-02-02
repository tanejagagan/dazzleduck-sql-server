package io.dazzleduck.sql.logback;


import ch.qos.logback.classic.spi.ILoggingEvent;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a single log entry with all required metadata for forwarding.
 */
public final class LogEntry {
    private final long sNo;
    private final Instant timestamp;
    private final String level;
    private final String logger;
    private final String thread;
    private final String message;
    private final Map<String, String> mdc;

    public LogEntry(long sNo, Instant timestamp, String level, String logger, String thread, String message) {
        this(sNo, timestamp, level, logger, thread, message, Collections.emptyMap());
    }

    public LogEntry(long sNo, Instant timestamp, String level, String logger, String thread, String message, Map<String, String> mdc) {
        this.sNo = sNo;
        this.timestamp = timestamp;
        this.level = level;
        this.logger = logger;
        this.thread = thread;
        this.message = message;
        this.mdc = mdc != null ? Map.copyOf(mdc) : Collections.emptyMap();
    }

    /**
     * Factory method to create LogEntry from Logback event.
     *
     * @param sequenceNumber The sequence number for this log entry
     * @param event The Logback logging event
     * @return A new LogEntry instance
     */
    public static LogEntry from(long sequenceNumber, ILoggingEvent event) {
        return new LogEntry(
                sequenceNumber,
                Instant.ofEpochMilli(event.getTimeStamp()),
                event.getLevel().toString(),
                event.getLoggerName(),
                event.getThreadName(),
                event.getFormattedMessage(),
                event.getMDCPropertyMap()
        );
    }

    public long sNo() {
        return sNo;
    }

    public Instant timestamp() {
        return timestamp;
    }

    public String level() {
        return level;
    }

    public String logger() {
        return logger;
    }

    public String thread() {
        return thread;
    }

    public String message() {
        return message;
    }

    public Map<String, String> mdc() {
        return mdc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntry logEntry = (LogEntry) o;
        return sNo == logEntry.sNo &&
               Objects.equals(timestamp, logEntry.timestamp) &&
               Objects.equals(level, logEntry.level) &&
               Objects.equals(logger, logEntry.logger) &&
               Objects.equals(thread, logEntry.thread) &&
               Objects.equals(message, logEntry.message) &&
               Objects.equals(mdc, logEntry.mdc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sNo, timestamp, level, logger, thread, message, mdc);
    }

    @Override
    public String toString() {
        return "LogEntry[" +
               "sNo=" + sNo +
               ", timestamp=" + timestamp +
               ", level=" + level +
               ", logger=" + logger +
               ", thread=" + thread +
               ", message=" + message +
               ", mdc=" + mdc +
               "]";
    }
}
