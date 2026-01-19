package io.dazzleduck.sql.logger.tailing.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Model for JSON log message structure
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class LogMessage {
    private final String timestamp;
    private final String level;
    private final String logger;
    private final String thread;
    private final String message;
    private final Map<String, String> mdc;
    private final String marker;

    @JsonCreator
    public LogMessage(
            @JsonProperty("timestamp") String timestamp,
            @JsonProperty("level") String level,
            @JsonProperty("logger") String logger,
            @JsonProperty("thread") String thread,
            @JsonProperty("message") String message,
            @JsonProperty("mdc") Map<String, String> mdc,
            @JsonProperty("marker") String marker) {
        // Timestamp is required
        if (timestamp == null || timestamp.isBlank()) {
            throw new IllegalArgumentException("timestamp cannot be null or empty");
        }
        this.timestamp = timestamp;

        // Level defaults to INFO if not provided
        this.level = (level == null || level.isBlank()) ? "INFO" : level;

        this.logger = logger;
        this.thread = thread;

        // Message defaults to empty string if not provided
        this.message = (message == null) ? "" : message;

        // MDC defaults to empty map if not provided
        this.mdc = (mdc == null) ? Collections.emptyMap() : mdc;

        this.marker = marker;
    }

    public String timestamp() {
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

    public String marker() {
        return marker;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogMessage that = (LogMessage) o;
        return Objects.equals(timestamp, that.timestamp) &&
               Objects.equals(level, that.level) &&
               Objects.equals(logger, that.logger) &&
               Objects.equals(thread, that.thread) &&
               Objects.equals(message, that.message) &&
               Objects.equals(mdc, that.mdc) &&
               Objects.equals(marker, that.marker);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, level, logger, thread, message, mdc, marker);
    }

    @Override
    public String toString() {
        return "LogMessage[" +
               "timestamp=" + timestamp +
               ", level=" + level +
               ", logger=" + logger +
               ", thread=" + thread +
               ", message=" + message +
               ", mdc=" + mdc +
               ", marker=" + marker +
               "]";
    }
}
