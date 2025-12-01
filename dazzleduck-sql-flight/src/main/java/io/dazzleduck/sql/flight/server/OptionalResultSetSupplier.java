package io.dazzleduck.sql.flight.server;

import org.duckdb.DuckDBResultSet;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public interface OptionalResultSetSupplier {
    boolean hasResultSet();

    DuckDBResultSet get() throws SQLException;

    void execute() throws SQLException;

    static OptionalResultSetSupplier of(final Statement statement, String query) {
        return new OptionalResultSetSupplier() {
            boolean hasResultSet;

            @Override
            public boolean hasResultSet() {
                return hasResultSet;
            }

            @Override
            public DuckDBResultSet get() throws SQLException {
                return (DuckDBResultSet) statement.getResultSet();
            }

            @Override
            public void execute() throws SQLException {
                hasResultSet = statement.execute(query);
            }
        };
    }

    static OptionalResultSetSupplier of(PreparedStatement preparedStatement) {
        return new OptionalResultSetSupplier() {
            boolean hasResultSet;

            @Override
            public boolean hasResultSet() {
                return hasResultSet;
            }

            @Override
            public DuckDBResultSet get() throws SQLException {
                return (DuckDBResultSet) preparedStatement.getResultSet();
            }

            @Override
            public void execute() throws SQLException {
                hasResultSet = preparedStatement.execute();
            }
        };
    }
}
