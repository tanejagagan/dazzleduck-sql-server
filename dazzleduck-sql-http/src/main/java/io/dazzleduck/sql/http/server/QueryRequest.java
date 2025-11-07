package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.flight.server.StatementHandle;

import javax.annotation.Nullable;

public record QueryRequest(String query, @Nullable Long id) {
    public QueryRequest(String query) {
        this(query, StatementHandle.nextStatementId());
    }
}
