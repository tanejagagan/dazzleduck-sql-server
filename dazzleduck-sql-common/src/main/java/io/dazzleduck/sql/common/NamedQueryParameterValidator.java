package io.dazzleduck.sql.common;

import java.util.Map;

public interface NamedQueryParameterValidator {
    void validate(Map<String, String> parameters) throws ParameterValidationException;

    default String description() {
        return "";
    }
}
