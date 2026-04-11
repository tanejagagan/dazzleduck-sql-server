package io.dazzleduck.sql.http.server.model;

import io.dazzleduck.sql.flight.server.StatementHandle;

import javax.annotation.Nullable;

public record QueryRequest(String query, @Nullable Long id, @Nullable Descriptor descriptor) {
    public QueryRequest(String query) {
        this(query, StatementHandle.nextStatementId(), null);
    }
}
