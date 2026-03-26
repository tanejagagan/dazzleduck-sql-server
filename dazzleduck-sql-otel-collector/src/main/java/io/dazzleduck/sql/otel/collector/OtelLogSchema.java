package io.dazzleduck.sql.otel.collector;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Arrow schema for OTLP log records, flattened from the 3-level OTLP hierarchy
 * (ResourceLogs → ScopeLogs → LogRecord).
 */
public class OtelLogSchema {

    // Column indices
    public static final int COL_TIMESTAMP = 0;
    public static final int COL_OBSERVED_TIMESTAMP = 1;
    public static final int COL_SEVERITY_NUMBER = 2;
    public static final int COL_SEVERITY_TEXT = 3;
    public static final int COL_BODY = 4;
    public static final int COL_TRACE_ID = 5;
    public static final int COL_SPAN_ID = 6;
    public static final int COL_FLAGS = 7;
    public static final int COL_EVENT_NAME = 8;
    public static final int COL_ATTRIBUTES = 9;
    public static final int COL_RESOURCE_ATTRIBUTES = 10;
    public static final int COL_SCOPE_NAME = 11;
    public static final int COL_SCOPE_VERSION = 12;

    public static final Schema SCHEMA = new Schema(List.of(
            // LogRecord fields
            new Field("timestamp",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null),
            new Field("observed_timestamp",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null),
            new Field("severity_number",
                    FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("severity_text",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("body",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("trace_id",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("span_id",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("flags",
                    FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("event_name",
                    FieldType.nullable(new ArrowType.Utf8()), null),

            // LogRecord.attributes: Map<String, String>
            mapField("attributes"),

            // Resource.attributes: Map<String, String>
            mapField("resource_attributes"),

            // InstrumentationScope fields
            new Field("scope_name",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("scope_version",
                    FieldType.nullable(new ArrowType.Utf8()), null)
    ));

    private static Field mapField(String name) {
        return new Field(name,
                FieldType.nullable(new ArrowType.Map(false)),
                List.of(new Field("entries",
                        FieldType.notNullable(new ArrowType.Struct()),
                        List.of(
                                new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                                new Field("value", FieldType.nullable(new ArrowType.Utf8()), null)
                        )
                ))
        );
    }
}
