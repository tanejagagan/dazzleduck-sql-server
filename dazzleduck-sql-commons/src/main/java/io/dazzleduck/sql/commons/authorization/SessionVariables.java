package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.Headers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Turns the {@link Headers#CLAIM_SESSION_VARIABLES} JWT claim — a JSON object of string
 * key/values, e.g. {@code {"tenant_id":"acme"}} — into DuckDB {@code SET VARIABLE} statements
 * applied to the per-request connection, so queries and injected row-level-security filters can
 * read them with {@code getvariable('name')}.
 *
 * <p>The claim is trusted only because it arrives inside the server-signed token (the caller must
 * read it from the verified claims, never from a client request header). Even so, names are
 * validated against a conservative identifier and values are rendered as escaped SQL string
 * literals, so a crafted claim cannot inject SQL.
 *
 * <p>All values are applied as VARCHAR literals, so every value must be a JSON string — a bare
 * number or boolean is rejected with a hint to quote it. A filter needing a numeric/temporal
 * comparison casts, e.g. {@code getvariable('n')::INT}.
 */
public final class SessionVariables {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** DuckDB variable name we allow to appear unquoted in {@code SET VARIABLE}. */
    private static final Pattern VALID_NAME = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

    private SessionVariables() {
    }

    /**
     * Renders {@code SET VARIABLE <name> = '<value>'} for each entry of the JSON object in
     * {@code claimJson}. Returns an empty list when the claim is null, blank, or an empty object.
     * Null-valued entries are skipped ({@code getvariable} of an unset name already returns NULL).
     *
     * @throws IllegalArgumentException if the claim is not a JSON object of scalar values, or a
     *                                  key is not a valid identifier
     */
    public static List<String> toSetStatements(String claimJson) {
        if (claimJson == null || claimJson.isBlank()) {
            return List.of();
        }
        Map<String, Object> vars;
        try {
            vars = MAPPER.readValue(claimJson, new TypeReference<LinkedHashMap<String, Object>>() {
            });
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid " + Headers.CLAIM_SESSION_VARIABLES + " claim: expected a JSON object", e);
        }
        List<String> sqls = new ArrayList<>(vars.size());
        for (Map.Entry<String, Object> entry : vars.entrySet()) {
            String name = entry.getKey();
            if (!VALID_NAME.matcher(name).matches()) {
                throw new IllegalArgumentException("Invalid session variable name: " + name);
            }
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }
            if (value instanceof Map || value instanceof List) {
                throw new IllegalArgumentException(
                        "Session variable '" + name + "' must be a JSON string, not a nested JSON value");
            }
            if (!(value instanceof String)) {
                // Every session variable is a VARCHAR, so numbers and booleans must be quoted in the
                // claim rather than coerced — reject them with a hint instead of guessing intent.
                throw new IllegalArgumentException(
                        "Session variable '" + name + "': " + value + " needs to be inside quotes "
                                + "(write it as \"" + name + "\": \"" + value + "\")");
            }
            sqls.add("SET VARIABLE " + name + " = " + sqlLiteral((String) value));
        }
        return sqls;
    }

    /** A single-quoted SQL string literal with embedded quotes doubled. */
    private static String sqlLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
