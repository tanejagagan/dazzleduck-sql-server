package io.dazzleduck.sql.common;

/** Canonical content types for query results, shared across modules and reusable by consumers. */
public final class ContentTypes {

    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_ARROW = "application/vnd.apache.arrow.stream";
    public static final String TEXT_TSV = "text/tab-separated-values";
    public static final String TEXT_TSV_UTF8 = "text/tab-separated-values; charset=utf-8";

    private ContentTypes() {
    }
}
