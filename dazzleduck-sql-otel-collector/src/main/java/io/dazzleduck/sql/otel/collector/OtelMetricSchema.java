package io.dazzleduck.sql.otel.collector;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Wide-table Arrow schema for all OTLP metric types (GAUGE, SUM, HISTOGRAM,
 * EXPONENTIAL_HISTOGRAM, SUMMARY). Columns not applicable to a metric type are null.
 */
public class OtelMetricSchema {

    public static final int COL_NAME = 0;
    public static final int COL_DESCRIPTION = 1;
    public static final int COL_UNIT = 2;
    public static final int COL_METRIC_TYPE = 3;
    public static final int COL_START_TIME_MS = 4;
    public static final int COL_TIME_MS = 5;
    public static final int COL_ATTRIBUTES = 6;
    public static final int COL_RESOURCE_ATTRIBUTES = 7;
    public static final int COL_SCOPE_NAME = 8;
    public static final int COL_SCOPE_VERSION = 9;
    public static final int COL_VALUE_DOUBLE = 10;
    public static final int COL_VALUE_INT = 11;
    public static final int COL_COUNT = 12;
    public static final int COL_SUM = 13;
    public static final int COL_BUCKET_COUNTS = 14;
    public static final int COL_EXPLICIT_BOUNDS = 15;
    public static final int COL_QUANTILE_VALUES = 16;
    public static final int COL_IS_MONOTONIC = 17;
    public static final int COL_AGGREGATION_TEMPORALITY = 18;

    public static final Schema SCHEMA = new Schema(List.of(
            new Field("name",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("description",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("unit",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("metric_type",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("start_time_ms",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null),
            new Field("time_ms",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null),
            mapField("attributes"),
            mapField("resource_attributes"),
            new Field("scope_name",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("scope_version",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("value_double",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
            new Field("value_int",
                    FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("count",
                    FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("sum",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
            new Field("bucket_counts",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("explicit_bounds",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("quantile_values",
                    FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("is_monotonic",
                    FieldType.nullable(new ArrowType.Bool()), null),
            new Field("aggregation_temporality",
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
