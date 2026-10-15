package io.dazzleduck.sql.otel.collector;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;
import java.util.Map;

/**
 * The attribute map columns written for one OTLP attribute set ({@code attributes},
 * {@code resource_attributes}, a span event's or link's {@code attributes}), keyed by the
 * {@code AnyValue} kind that lands in each.
 *
 * <p>{@link #STRING} is the original {@code MAP(VARCHAR, VARCHAR)} column, named after the
 * attribute set itself. The typed maps are suffixed and appended after the existing columns, so
 * column indices that predate them stay valid. Arrays and key/value lists both go to
 * {@link #JSON} as typed JSON text, in a Utf8 vector tagged with the {@code arrow.json} extension
 * name, which DuckDB reads as its {@code JSON} type and keeps through the Parquet write.
 */
enum AttributeType {
    STRING("", new ArrowType.Utf8()),
    INT("_int", new ArrowType.Int(64, true)),
    DOUBLE("_double", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
    BOOL("_bool", new ArrowType.Bool()),
    BYTES("_bytes", new ArrowType.Binary()),
    JSON("_json", new ArrowType.Utf8(), Map.of("ARROW:extension:name", "arrow.json"));

    /** Every type but {@link #STRING}, in column order. */
    static final List<AttributeType> TYPED = List.of(INT, DOUBLE, BOOL, BYTES, JSON);

    private final String suffix;
    private final ArrowType arrowType;
    private final Map<String, String> metadata;

    AttributeType(String suffix, ArrowType arrowType) {
        this(suffix, arrowType, null);
    }

    AttributeType(String suffix, ArrowType arrowType, Map<String, String> metadata) {
        this.suffix = suffix;
        this.arrowType = arrowType;
        this.metadata = metadata;
    }

    String suffix() {
        return suffix;
    }

    ArrowType arrowType() {
        return arrowType;
    }

    /** Field metadata for the map's value field; null when there is none. */
    Map<String, String> metadata() {
        return metadata;
    }
}
