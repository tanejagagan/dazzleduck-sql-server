package io.dazzleduck.sql.commons.delta;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * DataType Class
 * Provides utility methods and constants for working with SQL data types and casting rules.
 */
public final class DataType {

    // Private constructor to prevent instantiation
    private DataType() {
    }

    // Constants for data types
    public static final String TINY_INT = "TINYINT";
    public static final String SMALL_INT = "SMALLINT";
    public static final String INT = "INTEGER";
    public static final String BIG_INT = "BIGINT";
    public static final String DECIMAL = "DECIMAL";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String DATE = "DATE";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String TIMESTAMP_NTZ = "TIMESTAMP_NTZ";
    public static final String ARRAY = "ARRAY";
    public static final String MAP = "MAP";
    public static final String STRUCT = "STRUCT";
    public static final String VARIANT = "VARIANT";
    public static final String OBJECT = "OBJECT";
    public static final String BOOLEAN = "BOOLEAN";
    public static final String BINARY = "BLOB";
    public static final String INTERVAL = "INTERVAL";
    public static final String STRING = "STRING";

    // Data type categories
    private static final List<String> INTEGRAL_NUMERIC_TYPES = List.of(TINY_INT, SMALL_INT, INT, BIG_INT);
    private static final List<String> EXACT_NUMERIC_TYPES = List.of(DECIMAL);
    private static final List<String> BINARY_FLOATING_POINT_TYPES = List.of(FLOAT, DOUBLE);
    private static final List<String> NUMERIC_TYPES = Stream.of(INTEGRAL_NUMERIC_TYPES, EXACT_NUMERIC_TYPES, BINARY_FLOATING_POINT_TYPES)
            .flatMap(List::stream)
            .toList();
    private static final List<String> DATETIME_TYPES = List.of(DATE, TIMESTAMP, TIMESTAMP_NTZ);
    private static final List<String> COMPLEX_TYPES = List.of(ARRAY, MAP, STRUCT, VARIANT, OBJECT);

    private static final List<String> ALL_TYPES = Stream.of(
                    NUMERIC_TYPES.stream(),
                    DATETIME_TYPES.stream(),
                    COMPLEX_TYPES.stream(),
                    Stream.of(BOOLEAN),
                    Stream.of(BINARY),
                    Stream.of(INTERVAL),
                    Stream.of(STRING)
            )
            .flatMap(stream -> stream)
            .toList();

    // Casting rules
    private static final Map<String, List<String>> CAST_RULES = Map.ofEntries(
            // Numeric
            Map.entry(TINY_INT, Stream.of(NUMERIC_TYPES.stream(), Stream.of(STRING), Stream.of(TIMESTAMP), Stream.of(BOOLEAN), Stream.of(VARIANT)).flatMap(stream -> stream).collect(Collectors.toList())),
            Map.entry(SMALL_INT, Stream.of(NUMERIC_TYPES.stream(), Stream.of(STRING), Stream.of(TIMESTAMP), Stream.of(BOOLEAN), Stream.of(VARIANT)).flatMap(stream -> stream).collect(Collectors.toList())),
            Map.entry(INT, Stream.of(NUMERIC_TYPES.stream(), Stream.of(STRING), Stream.of(TIMESTAMP), Stream.of(BOOLEAN), Stream.of(VARIANT)).flatMap(stream -> stream).collect(Collectors.toList())),
            Map.entry(BIG_INT, Stream.of(NUMERIC_TYPES.stream(), Stream.of(STRING), Stream.of(TIMESTAMP), Stream.of(BOOLEAN), Stream.of(VARIANT)).flatMap(stream -> stream).collect(Collectors.toList())),
            Map.entry(DECIMAL, Stream.of(NUMERIC_TYPES.stream(), Stream.of(STRING), Stream.of(TIMESTAMP), Stream.of(BOOLEAN), Stream.of(VARIANT)).flatMap(stream -> stream).collect(Collectors.toList())),
            Map.entry(FLOAT, Stream.of(NUMERIC_TYPES.stream(), Stream.of(STRING), Stream.of(TIMESTAMP), Stream.of(BOOLEAN), Stream.of(VARIANT)).flatMap(stream -> stream).collect(Collectors.toList())),
            Map.entry(DOUBLE, Stream.of(NUMERIC_TYPES.stream(), Stream.of(STRING), Stream.of(TIMESTAMP), Stream.of(BOOLEAN), Stream.of(VARIANT)).flatMap(stream -> stream).collect(Collectors.toList())),
            // String
            Map.entry(STRING, ALL_TYPES),
            // DATE
            Map.entry(DATE, Stream.of(Stream.of(STRING), DATETIME_TYPES.stream(), Stream.of(VARIANT)).flatMap(stream -> stream).collect(Collectors.toList())),
            // TIMESTAMP
            Map.entry(TIMESTAMP, Stream.of(NUMERIC_TYPES.stream(), Stream.of(STRING), DATETIME_TYPES.stream(), Stream.of(VARIANT)).flatMap(stream -> stream).collect(Collectors.toList())),
            // TIMESTAMP_NTZ
            Map.entry(TIMESTAMP_NTZ, Stream.of(Stream.of(STRING), DATETIME_TYPES.stream(), Stream.of(VARIANT)).flatMap(stream -> stream).collect(Collectors.toList())),
            // BOOLEAN
            Map.entry(BOOLEAN, Stream.of(Stream.of(STRING), Stream.of(TIMESTAMP), Stream.of(BOOLEAN), NUMERIC_TYPES.stream(), Stream.of(VARIANT)).flatMap(stream -> stream).collect(Collectors.toList())),
            // BINARY
            Map.entry(BINARY, Stream.of(Stream.of(STRING), NUMERIC_TYPES.stream(), Stream.of(VARIANT), Stream.of(BINARY)).flatMap(stream -> stream).collect(Collectors.toList())),
            // ARRAY
            Map.entry(ARRAY, List.of(STRING, ARRAY, VARIANT)),
            // MAP
            Map.entry(MAP, List.of(STRING, MAP)),
            // STRUCT
            Map.entry(STRUCT, List.of(STRING, STRUCT)),
            // VARIANT
            Map.entry(VARIANT, List.of(STRING, VARIANT)),
            // OBJECT
            Map.entry(OBJECT, List.of(MAP, STRUCT))
    );

    // Public methods to access data type information
    public static List<String> getAllTypes() {
        return ALL_TYPES;
    }

    public static List<String> getNumericTypes() {
        return NUMERIC_TYPES;
    }

    public static List<String> getDatetimeTypes() {
        return DATETIME_TYPES;
    }

    public static List<String> getComplexTypes() {
        return COMPLEX_TYPES;
    }

    public static Map<String, List<String>> getCastRules() {
        return CAST_RULES;
    }
}