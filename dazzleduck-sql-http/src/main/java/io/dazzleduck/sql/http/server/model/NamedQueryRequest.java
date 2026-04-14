package io.dazzleduck.sql.http.server.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public record NamedQueryRequest(String name, Map<String, String> parameters, QueryMode mode) {

    @JsonCreator
    public NamedQueryRequest(
            @JsonProperty("name") String name,
            @JsonProperty("parameters") Map<String, String> parameters,
            @JsonProperty("mode") QueryMode mode) {
        this.name = name;
        this.parameters = parameters;
        this.mode = mode != null ? mode : QueryMode.EXECUTE;
    }

    /** Convenience factory for execution mode (no EXPLAIN prefix). */
    public static NamedQueryRequest execute(String name, Map<String, String> parameters) {
        return new NamedQueryRequest(name, parameters, QueryMode.EXECUTE);
    }
}
