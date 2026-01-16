package io.dazzleduck.sql.logback;


import ch.qos.logback.classic.spi.ILoggingEvent;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Represents a single log entry with all required metadata for forwarding.
 */
public record LogEntry(
        @JsonProperty("s_no") long sNo,
        @JsonProperty("timestamp") Instant timestamp,
        @JsonProperty("level") String level,
        @JsonProperty("logger") String logger,
        @JsonProperty("thread") String thread,
        @JsonProperty("message") String message
) {
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
}
