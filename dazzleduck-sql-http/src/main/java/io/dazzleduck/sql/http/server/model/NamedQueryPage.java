package io.dazzleduck.sql.http.server.model;

import java.util.List;

/** Paginated list of named queries. */
public record NamedQueryPage(List<NamedQueryListItem> items, long total, int offset, int limit) {

    /** Intermediate result from a single-query page+count fetch. */
    public record WithTotal(List<NamedQueryListItem> items, long total) {}
}
