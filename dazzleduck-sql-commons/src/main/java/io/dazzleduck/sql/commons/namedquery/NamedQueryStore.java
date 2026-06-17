package io.dazzleduck.sql.commons.namedquery;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.duckdb.DuckDBConnection;

import java.sql.SQLException;
import java.util.Optional;

/**
 * Loads {@link NamedQueryDefinition} rows from a DuckDB-backed template table via
 * {@link ConnectionPool}. Pure data access — no Flight or templating dependency — so any
 * named-query consumer can resolve a template by name and then render/execute it however it likes.
 */
public final class NamedQueryStore {

    private static final String LOAD_BY_NAME =
            "SELECT id, name, template, validators, description, parameter_descriptions, preferred_display, query_group FROM %s WHERE name = '%s'";

    private NamedQueryStore() {
    }

    /**
     * Loads the named query {@code name} from {@code table}, or {@link Optional#empty()} when no row
     * matches. The name is single-quote-escaped before interpolation; the caller owns {@code table}
     * (it is interpolated verbatim, so it must be a trusted identifier).
     */
    public static Optional<NamedQueryDefinition> loadByName(String table, String name) throws SQLException {
        String safeName = name.replace("'", "''");
        String sql = LOAD_BY_NAME.formatted(table, safeName);
        try (DuckDBConnection connection = ConnectionPool.getConnection()) {
            var iterable = ConnectionPool.collectAll(connection, sql, NamedQueryDefinition.class);
            var iter = iterable.iterator();
            try {
                return iter.hasNext() ? Optional.of(iter.next()) : Optional.empty();
            } finally {
                if (iter instanceof AutoCloseable ac) {
                    try { ac.close(); } catch (Exception ignored) {}
                }
            }
        }
    }
}
