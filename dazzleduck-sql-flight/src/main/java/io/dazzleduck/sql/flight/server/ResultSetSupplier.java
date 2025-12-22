package io.dazzleduck.sql.flight.server;

import org.duckdb.DuckDBResultSet;

import java.sql.SQLException;

public interface ResultSetSupplier {
    DuckDBResultSet get() throws SQLException;
}
