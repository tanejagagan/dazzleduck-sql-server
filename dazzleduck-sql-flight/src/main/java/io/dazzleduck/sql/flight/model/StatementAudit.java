package io.dazzleduck.sql.flight.model;

import java.time.Instant;

public record StatementAudit(
        Long statementId,             // Query ID
        String user,                  // User Name
        String action,                // CANCEL, START, END, ERROR, TIMEOUT ETC.
        boolean isPreparedStatement,  // True or False
        String query,
        Instant startTime,
        Instant endTime,
        long bytesOut,
        String error
) {}
