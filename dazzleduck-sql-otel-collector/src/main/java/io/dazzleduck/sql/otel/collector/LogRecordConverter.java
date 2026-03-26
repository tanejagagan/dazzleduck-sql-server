package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.dazzleduck.sql.common.types.JavaRow;

import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Flattens an OTLP LogRecord (with its Resource and InstrumentationScope context)
 * into a single JavaRow matching OtelLogSchema.
 */
public class LogRecordConverter {

    private static final HexFormat HEX = HexFormat.of();

    public static JavaRow toRow(LogRecord record, Resource resource, InstrumentationScope scope) {
        Object[] values = new Object[13];

        // Timestamps: nanoseconds → milliseconds
        values[OtelLogSchema.COL_TIMESTAMP] = record.getTimeUnixNano() > 0
                ? record.getTimeUnixNano() / 1_000_000L
                : null;
        values[OtelLogSchema.COL_OBSERVED_TIMESTAMP] = record.getObservedTimeUnixNano() > 0
                ? record.getObservedTimeUnixNano() / 1_000_000L
                : null;

        values[OtelLogSchema.COL_SEVERITY_NUMBER] = record.getSeverityNumberValue();
        values[OtelLogSchema.COL_SEVERITY_TEXT] = emptyToNull(record.getSeverityText());
        values[OtelLogSchema.COL_BODY] = record.hasBody() ? anyValueToString(record.getBody()) : null;

        // trace_id / span_id: raw bytes → lowercase hex
        byte[] traceId = record.getTraceId().toByteArray();
        values[OtelLogSchema.COL_TRACE_ID] = traceId.length > 0 ? HEX.formatHex(traceId) : null;

        byte[] spanId = record.getSpanId().toByteArray();
        values[OtelLogSchema.COL_SPAN_ID] = spanId.length > 0 ? HEX.formatHex(spanId) : null;

        values[OtelLogSchema.COL_FLAGS] = (int) (record.getFlags() & 0xFFFFFFFFL);
        values[OtelLogSchema.COL_EVENT_NAME] = null; // field added in proto > 1.3.2

        values[OtelLogSchema.COL_ATTRIBUTES] = kvListToMap(record.getAttributesList());
        values[OtelLogSchema.COL_RESOURCE_ATTRIBUTES] = resource != null
                ? kvListToMap(resource.getAttributesList())
                : new LinkedHashMap<>();

        values[OtelLogSchema.COL_SCOPE_NAME] = scope != null ? emptyToNull(scope.getName()) : null;
        values[OtelLogSchema.COL_SCOPE_VERSION] = scope != null ? emptyToNull(scope.getVersion()) : null;

        return new JavaRow(values);
    }

    /**
     * Converts a repeated KeyValue list to Map&lt;String, String&gt;.
     * AnyValue is stringified; nested types (array, kvlist) are rendered as JSON.
     */
    static Map<String, String> kvListToMap(List<KeyValue> kvList) {
        Map<String, String> map = new LinkedHashMap<>();
        for (KeyValue kv : kvList) {
            map.put(kv.getKey(), anyValueToString(kv.getValue()));
        }
        return map;
    }

    /**
     * Converts an OTLP AnyValue to its string representation.
     * Nested types (array, kvlist) are rendered as JSON strings.
     */
    static String anyValueToString(AnyValue value) {
        if (value == null) return null;
        return switch (value.getValueCase()) {
            case STRING_VALUE -> value.getStringValue();
            case BOOL_VALUE   -> String.valueOf(value.getBoolValue());
            case INT_VALUE    -> String.valueOf(value.getIntValue());
            case DOUBLE_VALUE -> String.valueOf(value.getDoubleValue());
            case ARRAY_VALUE  -> arrayToJson(value);
            case KVLIST_VALUE -> kvlistToJson(value);
            case BYTES_VALUE  -> HEX.formatHex(value.getBytesValue().toByteArray());
            default           -> "";
        };
    }

    private static String arrayToJson(AnyValue value) {
        StringBuilder sb = new StringBuilder("[");
        var items = value.getArrayValue().getValuesList();
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) sb.append(",");
            appendJsonString(sb, anyValueToString(items.get(i)));
        }
        return sb.append("]").toString();
    }

    private static String kvlistToJson(AnyValue value) {
        StringBuilder sb = new StringBuilder("{");
        var entries = value.getKvlistValue().getValuesList();
        for (int i = 0; i < entries.size(); i++) {
            if (i > 0) sb.append(",");
            KeyValue kv = entries.get(i);
            appendJsonString(sb, kv.getKey());
            sb.append(":");
            appendJsonString(sb, anyValueToString(kv.getValue()));
        }
        return sb.append("}").toString();
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
                default   -> sb.append(c);
            }
        }
        sb.append('"');
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isEmpty()) ? null : s;
    }
}
