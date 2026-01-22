package io.dazzleduck.sql.http.server.model;

import io.dazzleduck.sql.flight.server.StatementHandle;

public record Descriptor(StatementHandle statementHandle) {
}
