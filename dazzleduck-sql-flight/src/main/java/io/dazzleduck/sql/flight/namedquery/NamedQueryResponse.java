package io.dazzleduck.sql.flight.namedquery;

import java.util.List;
import java.util.Map;

/**
 * Full view of a named query returned by {@code GET /named-query/{name}}.
 *
 * <p>The raw SQL template is omitted. Validator class names are replaced by the
 * human-readable descriptions from
 * {@link io.dazzleduck.sql.common.NamedQueryParameterValidator#description()}.
 */
public record NamedQueryResponse(
        long id,
        String name,
        String description,
        Map<String, String> parameterDescriptions,
        List<String> validatorDescriptions) {
}
