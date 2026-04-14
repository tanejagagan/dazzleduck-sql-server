package io.dazzleduck.sql.http.server.model;

/** Minimal projection of a named query returned in paginated list responses. */
public record NamedQueryListItem(String name, String description) {
}
