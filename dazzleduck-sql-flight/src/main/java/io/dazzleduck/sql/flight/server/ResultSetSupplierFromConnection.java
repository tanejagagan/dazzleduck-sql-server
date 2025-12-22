package io.dazzleduck.sql.flight.server;

import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;

import java.sql.SQLException;

public interface ResultSetSupplierFromConnection {
    DuckDBResultSet get(DuckDBConnection connection) throws SQLException;
}
