package io.dazzleduck.sql.http.server.model;

import java.util.Map;

/**
 * DB-facing representation of a named query.
 *
 * <p>Maps directly to a row in the named-query table:
 * <pre>{@code
 *   CREATE TABLE named_queries (
 *       name                   VARCHAR PRIMARY KEY,
 *       template               VARCHAR,
 *       validators             VARCHAR[],
 *       description            VARCHAR,
 *       parameter_descriptions MAP(VARCHAR, VARCHAR)
 *   );
 * }</pre>
 *
 * <p>Column order must match the record component order because
 * {@code ConnectionPool.collectAll} maps by position.
 */
public record NamedQueryTemplate(
        String name,
        String template,
        String[] validators,
        String description,
        Map<String, String> parameterDescriptions) {
}
