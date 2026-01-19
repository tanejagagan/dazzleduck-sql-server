package io.dazzleduck.sql.logback;


import ch.qos.logback.classic.spi.ILoggingEvent;

import java.time.Instant;
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

    public LogEntry(long sNo, Instant timestamp, String level, String logger, String thread, String message) {
        this.sNo = sNo;
        this.timestamp = timestamp;
        this.level = level;
        this.logger = logger;
        this.thread = thread;
        this.message = message;
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
                event.getFormattedMessage()
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
               Objects.equals(message, logEntry.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sNo, timestamp, level, logger, thread, message);
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
               "]";
    }
}
