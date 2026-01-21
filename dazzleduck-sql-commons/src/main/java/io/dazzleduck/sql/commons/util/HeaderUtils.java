package io.dazzleduck.sql.commons.util;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class HeaderUtils {

    /**
     * Pattern to detect SQL comment sequences.
     */
    private static final Pattern COMMENT_PATTERN = Pattern.compile("--|/\\*|\\*/");

    /**
     * Pattern to detect semicolons (statement separators).
     */
    private static final Pattern SEMICOLON_PATTERN = Pattern.compile(";");

    /**
     * Pattern to detect dangerous SQL keywords surrounded by whitespace or at boundaries.
     * Keywords: SELECT, INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, EXEC, EXECUTE,
     * UNION, INTO, FROM, WHERE, GRANT, REVOKE, COPY, LOAD, ATTACH, DETACH
     */
    private static final Pattern DANGEROUS_KEYWORD_PATTERN = Pattern.compile(
            "(^|\\s)(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|EXEC|EXECUTE|" +
            "UNION|INTO|FROM|WHERE|GRANT|REVOKE|COPY|LOAD|ATTACH|DETACH)(\\s|$)",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Pattern to parse sort order entries: column name with optional ASC/DESC direction.
     * Group 1: column name (required)
     * Group 2: direction (optional, ASC or DESC)
     */
    private static final Pattern SORT_ORDER_PATTERN = Pattern.compile(
            "^(.+?)\\s+(ASC|DESC)$",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Parses CSV value into array of strings.
     * This is the base parsing method without validation.
     */
    public static String[] parseCsv(String value) {
        if (value == null || value.isBlank()) {
            return new String[0];
        }
        List<String> result = new ArrayList<>();
        try (CsvReader<CsvRecord> reader = CsvReader.builder().ofCsvRecord(new StringReader(value))) {
            for (CsvRecord record : reader) {
                for (int i = 0; i < record.getFieldCount(); i++) {
                    String field = record.getField(i).trim();
                    if (!field.isEmpty()) {
                        result.add(field);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse CSV value: " + value, e);
        }
        return result.toArray(new String[0]);
    }

    /**
     * Parses CSV value as column references for partition headers.
     * Returns the column names wrapped in double quotes for safe SQL usage.
     *
     * @param value CSV string with column names
     * @return array of double-quoted column references
     */
    public static String[] parseColumnReferences(String value) {
        String[] parsed = parseCsv(value);
        String[] result = new String[parsed.length];
        for (int i = 0; i < parsed.length; i++) {
            result[i] = quoteIdentifier(parsed[i]);
        }
        return result;
    }

    /**
     * Parses CSV value as sort order entries for sort_order header.
     * Each entry can be a column name optionally followed by ASC or DESC.
     * Returns the column names wrapped in double quotes with direction appended.
     *
     * @param value CSV string with sort order entries (e.g., "col1 ASC,col2 DESC,col3")
     * @return array of quoted column references with direction (e.g., ["\"col1\" ASC", "\"col2\" DESC", "\"col3\""])
     */
    public static String[] parseSortOrder(String value) {
        String[] parsed = parseCsv(value);
        String[] result = new String[parsed.length];
        for (int i = 0; i < parsed.length; i++) {
            result[i] = parseSortOrderEntry(parsed[i]);
        }
        return result;
    }

    /**
     * Parses a single sort order entry (column name with optional ASC/DESC).
     *
     * @param entry the sort order entry to parse
     * @return quoted column reference with optional direction
     */
    private static String parseSortOrderEntry(String entry) {
        var matcher = SORT_ORDER_PATTERN.matcher(entry.trim());
        if (matcher.matches()) {
            String columnName = matcher.group(1).trim();
            String direction = matcher.group(2).toUpperCase();
            return quoteIdentifier(columnName) + " " + direction;
        }
        // No direction specified, just quote the column name
        return quoteIdentifier(entry);
    }

    /**
     * Wraps an identifier in double quotes for safe SQL usage.
     * Escapes any existing double quotes by doubling them.
     *
     * @param identifier the identifier to quote
     * @return the quoted identifier
     */
    public static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * Parses CSV value as expressions for the project header.
     * Allows expressions but validates against dangerous SQL patterns.
     *
     * @param value CSV string with expressions
     * @return array of expressions
     * @throws IllegalArgumentException if any expression contains dangerous patterns
     */
    public static String[] parseExpressions(String value) {
        String[] parsed = parseCsv(value);
        for (String field : parsed) {
            validateExpression(field);
        }
        return parsed;
    }

    /**
     * Validates an expression for dangerous SQL injection patterns.
     * Checks for:
     * - SQL keywords surrounded by whitespace
     * - SQL comment sequences (-- and block comments)
     * - Semicolons (statement separators)
     *
     * @param expression the expression to validate
     * @throws IllegalArgumentException if dangerous patterns are detected
     */
    public static void validateExpression(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("Expression cannot be blank");
        }

        // Check for comment patterns
        if (COMMENT_PATTERN.matcher(expression).find()) {
            throw new IllegalArgumentException("SQL comments not allowed in expression: " + expression);
        }

        // Check for semicolons
        if (SEMICOLON_PATTERN.matcher(expression).find()) {
            throw new IllegalArgumentException("Semicolons not allowed in expression: " + expression);
        }

        // Check for dangerous keywords surrounded by whitespace or at boundaries
        var matcher = DANGEROUS_KEYWORD_PATTERN.matcher(expression);
        if (matcher.find()) {
            throw new IllegalArgumentException(
                    "Dangerous SQL keyword '" + matcher.group(2).toUpperCase() + "' detected in expression: " + expression);
        }
    }
}
