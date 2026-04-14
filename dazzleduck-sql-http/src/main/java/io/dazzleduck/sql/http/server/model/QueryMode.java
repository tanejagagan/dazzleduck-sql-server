package io.dazzleduck.sql.http.server.model;

/** Controls how a named query is executed. */
public enum QueryMode {
    /** Execute the query and return results (default). */
    EXECUTE,
    /** Prepend {@code EXPLAIN} — returns the query plan without executing. */
    EXPLAIN,
    /** Prepend {@code EXPLAIN ANALYZE} — executes the query and returns the annotated plan. */
    EXPLAIN_ANALYZE
}
