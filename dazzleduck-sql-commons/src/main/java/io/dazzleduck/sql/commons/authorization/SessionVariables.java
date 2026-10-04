package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.SqlVariables;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns the {@link Headers#CLAIM_SESSION_VARIABLES} JWT claim — a JSON object of string
 * key/values, e.g. {@code {"tenant_id":"acme"}} — into DuckDB {@code SET VARIABLE} statements
 * applied to the per-request connection, so queries and injected row-level-security filters can
 * read them with {@code getvariable('name')}.
 *
 * <p>The claim is trusted only because it arrives inside the server-signed token (the caller must
 * read it from the verified claims, never from a client request header). Even so, names are
 * validated against a conservative identifier and values are rendered as escaped SQL string
 * literals, so a crafted claim cannot inject SQL. Those rules — and the bounds on how many
 * variables and how long a value may be — live in {@link SqlVariables}, shared with the ingestion
 * path's per-queue variables. This class owns what is specific to the claim: parsing its JSON and
 * the {@link #validate} policy hook.
 *
 * <p>All values are applied as VARCHAR literals, so every value must be a JSON string — a bare
 * number or boolean is rejected with a hint to quote it. A filter needing a numeric/temporal
 * comparison casts, e.g. {@code getvariable('n')::INT}.
 */
public final class SessionVariables {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Names the variables in failure messages, so a rejected claim reads as the client's own. */
    private static final String NOUN = "session variable";

    private SessionVariables() {
    }

    /**
     * Renders {@code SET VARIABLE <name> = '<value>'} for each entry of the JSON object in
     * {@code claimJson}. Returns an empty list when the claim is null, blank, or an empty object.
     * Null-valued entries are skipped ({@code getvariable} of an unset name already returns NULL).
     *
     * <p>The claim value is the JSON <em>text</em>, i.e. a JWT claim whose value is a string such as
     * {@code "{\"tenant_id\":\"acme\"}"} — not a nested JSON object, which
     * {@code JwtClaimsExtractor} cannot read as a String.
     *
     * @throws IllegalArgumentException if the claim is not a JSON object of scalar values, a key is
     *                                  not a valid identifier, a value holds a control character,
     *                                  or the claim exceeds {@link #MAX_VARIABLES} entries or
     *                                  {@link #MAX_VALUE_LENGTH} characters in a value
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
        // Parse and enforce the structural rules needed for safe SQL rendering, collecting a
        // name→value map, then hand it to the policy hook before anything is applied.
        Map<String, String> variables = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : vars.entrySet()) {
            String name = entry.getKey();
            SqlVariables.validateName(name, NOUN);
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
            variables.put(name, (String) value);
        }
        // Name, value and count rules (shared with the ingestion path) before the policy hook, so
        // the hook only ever sees a structurally valid set.
        SqlVariables.validate(variables, NOUN);

        validate(variables);

        return SqlVariables.toSetStatements(variables, NOUN);
    }

    /**
     * Policy hook for the requested session variables, called after structural parsing and before
     * any {@code SET VARIABLE} is rendered. The parser already guarantees valid identifier names and
     * string values (what safe SQL rendering needs); this is where higher-level policy belongs —
     * e.g. an allow-list of variable names, per-tenant value constraints, or required variables.
     *
     * <p>Placeholder: the default accepts every variable. Implement policy checks here and throw
     * {@link IllegalArgumentException} to reject a variable set; the request then fails cleanly.
     *
     * @param variables the requested variables (insertion order preserved), all names valid
     *                  identifiers and all values non-null strings
     */
    static void validate(Map<String, String> variables) {
        // TODO: enforce session-variable policy (allowed names, value format/limits, per-tenant
        // rules). No additional checks yet.
    }
}
