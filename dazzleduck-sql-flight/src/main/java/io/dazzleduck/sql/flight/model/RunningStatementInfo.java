package io.dazzleduck.sql.flight.model;

import java.time.Duration;
import java.time.Instant;

public record RunningStatementInfo(
        String user,
        String statementId,
        Instant startInstant
) {
    public static String query;
    public static String action = "RUNNING";
    public static Instant endInstant = null;

    public RunningStatementInfo(String user, String statementId, String query) {
        this(user, statementId, Instant.now());
        RunningStatementInfo.query = query;
    }

    public long durationMs() {
        Instant end = (endInstant != null) ? endInstant : Instant.now();
        return Duration.between(startInstant, end).toMillis();
    }

    public Duration duration() {
        Instant end = (endInstant != null) ? endInstant : Instant.now();
        return Duration.between(startInstant, end);
    }
}
