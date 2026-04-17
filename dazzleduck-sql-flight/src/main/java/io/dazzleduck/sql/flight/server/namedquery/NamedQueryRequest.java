package io.dazzleduck.sql.flight.server.namedquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public record NamedQueryRequest(String name, Map<String, String> parameters) {

    @JsonCreator
    public NamedQueryRequest(
            @JsonProperty("name") String name,
            @JsonProperty("parameters") Map<String, String> parameters) {
        this.name = name;
        this.parameters = parameters;
    }

    /** Convenience factory for execution mode. */
    public static NamedQueryRequest execute(String name, Map<String, String> parameters) {
        return new NamedQueryRequest(name, parameters);
    }
}
