package io.dazzleduck.sql.otel.collector;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The batch writers address columns by the literal {@code COL_*} indices, so a column inserted or
 * moved in a schema would silently write values into the wrong column. This pins every index to
 * the column it names: {@code COL_START_TIME_MS} must be {@code start_time_ms}, and
 * {@code COL_TYPED_ATTRIBUTES} must start the five typed maps {@code attributes_int} ...
 * {@code attributes_json}, in {@link AttributeType#TYPED} order.
 */
class SchemaColumnIndexTest {

    @ParameterizedTest
    @ValueSource(classes = {OtelLogSchema.class, OtelTraceSchema.class, OtelMetricSchema.class})
    void everyColumnIndexNamesItsColumn(Class<?> schemaClass) throws Exception {
        Schema schema = (Schema) schemaClass.getField("SCHEMA").get(null);
        int checked = 0;
        for (Field constant : schemaClass.getFields()) {
            if (constant.getType() != int.class || !Modifier.isStatic(constant.getModifiers())
                    || !constant.getName().startsWith("COL_")) {
                continue;
            }
            int index = constant.getInt(null);
            String column = constant.getName().substring("COL_".length()).toLowerCase(Locale.ROOT);
            if (column.startsWith("typed_")) {
                String prefix = column.substring("typed_".length());
                for (int i = 0; i < AttributeType.TYPED.size(); i++) {
                    assertEquals(prefix + AttributeType.TYPED.get(i).suffix(),
                            schema.getFields().get(index + i).getName(), constant.getName() + " + " + i);
                }
            } else {
                assertEquals(column, schema.getFields().get(index).getName(), constant.getName());
            }
            checked++;
        }
        assertTrue(checked > 0, "no COL_ constants found on " + schemaClass.getSimpleName());
    }
}
