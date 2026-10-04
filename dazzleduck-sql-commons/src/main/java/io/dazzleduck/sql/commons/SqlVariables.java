package io.dazzleduck.sql.commons;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Renders DuckDB {@code SET VARIABLE} statements from name/value pairs that come from
 * configuration or from a verified token — never from SQL written by the caller.
 *
 * <p>Two sources use this: the query path's session variables (from the {@code x-dd-variables}
 * JWT claim, see {@code SessionVariables}) and the ingestion path's per-queue variables (from the
 * config file or a key/value relation, see {@code ingestion.IngestionVariables}). Both end up as
 * {@code SET VARIABLE} on the connection that runs the statement, readable with
 * {@code getvariable('name')}, so both need the same guarantees: a name that is a plain
 * identifier, a value that survives as one string literal, and bounds that stop one request or
 * one config entry from turning into thousands of statements.
 *
 * <p>The {@code noun} argument only names the thing in error messages ("session variable",
 * "ingestion variable"), so a failure points at the source the operator or client controls.
 */
public final class SqlVariables {

    /**
     * Variable name we accept. Deliberately excludes the double quote, so the name can be rendered
     * as a quoted identifier without any escaping of its own.
     */
    private static final Pattern VALID_NAME = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

    /**
     * A relation or column name interpolated into SQL: a plain identifier, optionally catalog- and
     * schema-qualified. Names that would need quoting are rejected rather than escaped.
     */
    private static final Pattern VALID_IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*(\\.[A-Za-z_][A-Za-z0-9_$]*){0,2}");

    /** Upper bounds so an oversized source cannot turn one write into thousands of statements. */
    public static final int MAX_VARIABLES = 64;
    public static final int MAX_VALUE_LENGTH = 4096;

    private SqlVariables() {
    }

    /**
     * Renders {@code SET VARIABLE "<name>" = '<value>'} for each entry, in iteration order.
     * Returns an empty list for a null or empty map.
     *
     * @throws IllegalArgumentException if a name is not a valid identifier, a value is null, too
     *                                  long or holds a control character, or there are more than
     *                                  {@link #MAX_VARIABLES} entries
     */
    public static List<String> toSetStatements(Map<String, String> variables, String noun) {
        if (variables == null || variables.isEmpty()) {
            return List.of();
        }
        validate(variables, noun);
        List<String> sqls = new ArrayList<>(variables.size());
        variables.forEach((name, value) ->
                sqls.add("SET VARIABLE " + quotedIdentifier(name) + " = " + sqlLiteral(value)));
        return sqls;
    }

    /**
     * Checks every name and value against the rules {@link #toSetStatements} renders under, without
     * rendering. Lets a source validate when it loads (where the message can name the file or
     * relation the pair came from) instead of at the write that first uses it.
     *
     * @throws IllegalArgumentException on the first entry that breaks a rule
     */
    public static void validate(Map<String, String> variables, String noun) {
        if (variables == null || variables.isEmpty()) {
            return;
        }
        if (variables.size() > MAX_VARIABLES) {
            throw new IllegalArgumentException("Too many " + noun + "s: " + variables.size()
                    + ", limit is " + MAX_VARIABLES);
        }
        variables.forEach((name, value) -> {
            validateName(name, noun);
            if (value == null) {
                throw new IllegalArgumentException(capitalize(noun) + " '" + name + "' has no value");
            }
            if (value.length() > MAX_VALUE_LENGTH) {
                throw new IllegalArgumentException(capitalize(noun) + " '" + name + "' is too long: "
                        + value.length() + " characters, limit is " + MAX_VALUE_LENGTH);
            }
            // DuckDB truncates a SQL string at an embedded NUL, which turns the rendered statement
            // into an unterminated literal and fails the whole request with an unrelated parser
            // error. Reject control characters here, while the message can still name the variable.
            int control = indexOfControlCharacter(value);
            if (control >= 0) {
                throw new IllegalArgumentException(String.format(
                        "%s '%s' contains a control character (U+%04X) at index %d",
                        capitalize(noun), name, (int) value.charAt(control), control));
            }
        });
    }

    /**
     * Whether {@code name} can be used as a variable name. A source reading from a shared relation
     * uses this to tell a row addressed to someone else from one addressed to it.
     */
    public static boolean isValidName(String name) {
        return name != null && VALID_NAME.matcher(name).matches();
    }

    /** @throws IllegalArgumentException if {@code name} is not a valid variable name */
    public static void validateName(String name, String noun) {
        if (!isValidName(name)) {
            throw new IllegalArgumentException("Invalid " + noun + " name: " + name);
        }
    }

    /**
     * Checks that {@code value} is a plain (optionally catalog- and schema-qualified) identifier,
     * for a relation or column name that is interpolated into SQL rather than bound.
     *
     * @param field the config key being checked, named in the failure message
     * @return {@code value}, so it can be used inline
     * @throws IllegalArgumentException when it is not such an identifier
     */
    public static String identifier(String field, String value) {
        if (value == null || !VALID_IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException(field + " must be a plain identifier"
                    + " (optionally catalog- and schema-qualified), got: " + value);
        }
        return value;
    }

    /**
     * A double-quoted SQL identifier. {@link #VALID_NAME} already rejects the double quote, so there
     * is nothing to escape. Quoting matters because the regex accepts reserved words — bare
     * {@code SET VARIABLE table = 'x'} is a DuckDB parser error, {@code SET VARIABLE "table" = 'x'}
     * is not.
     */
    public static String quotedIdentifier(String name) {
        return "\"" + name + "\"";
    }

    /** A single-quoted SQL string literal with embedded quotes doubled. */
    public static String sqlLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /** Index of the first ISO control character in {@code value}, or -1 if there is none. */
    private static int indexOfControlCharacter(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (Character.isISOControl(value.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    /** {@code "session variable"} → {@code "Session variable"}, for a message that starts with it. */
    private static String capitalize(String noun) {
        return noun.isEmpty() ? noun : Character.toUpperCase(noun.charAt(0)) + noun.substring(1);
    }
}
