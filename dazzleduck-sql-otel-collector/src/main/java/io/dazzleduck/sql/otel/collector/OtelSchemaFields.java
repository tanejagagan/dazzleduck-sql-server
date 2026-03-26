package io.dazzleduck.sql.otel.collector;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;

/**
 * Shared Arrow field builder helpers used across otel signal schemas.
 */
class OtelSchemaFields {

    static Field mapField(String name) {
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

    static Field listField(String name, ArrowType elementType) {
        return new Field(name,
                FieldType.nullable(new ArrowType.List()),
                List.of(new Field("item", FieldType.nullable(elementType), null)));
    }
}
