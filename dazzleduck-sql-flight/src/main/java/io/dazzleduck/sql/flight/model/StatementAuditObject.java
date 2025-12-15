package io.dazzleduck.sql.flight.model;

import java.time.Instant;

public record StatementAuditObject(
        Long statementId,
        String user,
        String action,           // CANCEL
        boolean prepared,
        String query,
        Instant startTime,
        Instant endTime,
        long bytesOut,
        String error
) {}
