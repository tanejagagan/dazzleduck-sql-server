package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;

import java.util.Base64;
import java.util.HexFormat;

/**
 * Renders OTLP values for the batch writers: {@link #anyValueToString} for string columns and
 * the string attribute maps, {@link #anyValueToJson} for the JSON attribute maps.
 */
public class LogRecordConverter {

    private static final HexFormat HEX = HexFormat.of();

    /**
     * Converts an OTLP AnyValue to its string representation.
     * Nested types (array, kvlist) are rendered as typed JSON, as {@link #anyValueToJson} does.
     */
    static String anyValueToString(AnyValue value) {
        if (value == null) return null;
        return switch (value.getValueCase()) {
            case STRING_VALUE -> value.getStringValue();
            case BOOL_VALUE   -> String.valueOf(value.getBoolValue());
            case INT_VALUE    -> String.valueOf(value.getIntValue());
            case DOUBLE_VALUE -> String.valueOf(value.getDoubleValue());
            case ARRAY_VALUE, KVLIST_VALUE -> anyValueToJson(value);
            case BYTES_VALUE  -> HEX.formatHex(value.getBytesValue().toByteArray());
            default           -> "";
        };
    }

    /**
     * Renders an OTLP AnyValue as JSON, keeping its type: numbers and booleans are bare, strings
     * quoted, an array a JSON array, a kvlist a JSON object (a repeated key keeps both entries, in
     * order), and an unset value {@code null}. Bytes are a base64 string, as in OTLP's own JSON
     * encoding. A non-finite double, which JSON cannot represent, is the string {@code "NaN"},
     * {@code "Infinity"} or {@code "-Infinity"}.
     */
    static String anyValueToJson(AnyValue value) {
        StringBuilder sb = new StringBuilder();
        appendJson(sb, value);
        return sb.toString();
    }

    private static void appendJson(StringBuilder sb, AnyValue value) {
        switch (value.getValueCase()) {
            case STRING_VALUE -> appendJsonString(sb, value.getStringValue());
            case BOOL_VALUE   -> sb.append(value.getBoolValue());
            case INT_VALUE    -> sb.append(value.getIntValue());
            case DOUBLE_VALUE -> {
                double d = value.getDoubleValue();
                if (Double.isFinite(d)) sb.append(d); else appendJsonString(sb, String.valueOf(d));
            }
            case BYTES_VALUE  -> appendJsonString(sb,
                    Base64.getEncoder().encodeToString(value.getBytesValue().toByteArray()));
            case ARRAY_VALUE  -> {
                sb.append('[');
                var items = value.getArrayValue().getValuesList();
                for (int i = 0; i < items.size(); i++) {
                    if (i > 0) sb.append(',');
                    appendJson(sb, items.get(i));
                }
                sb.append(']');
            }
            case KVLIST_VALUE -> {
                sb.append('{');
                var entries = value.getKvlistValue().getValuesList();
                for (int i = 0; i < entries.size(); i++) {
                    if (i > 0) sb.append(',');
                    KeyValue kv = entries.get(i);
                    appendJsonString(sb, kv.getKey());
                    sb.append(':');
                    appendJson(sb, kv.getValue());
                }
                sb.append('}');
            }
            default -> sb.append("null");
        }
    }

    private static void appendJsonString(StringBuilder sb, String s) {
        if (s == null) {
            sb.append("null");
            return;
        }
        sb.append('"');
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"'  -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default   -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c)); else sb.append(c);
                }
            }
        }
        sb.append('"');
    }

    static String emptyToNull(String s) {
        return (s == null || s.isEmpty()) ? null : s;
    }
}
