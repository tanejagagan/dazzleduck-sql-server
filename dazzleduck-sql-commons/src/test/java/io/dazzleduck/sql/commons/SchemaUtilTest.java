package io.dazzleduck.sql.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dazzleduck.sql.common.types.DataType;
import io.dazzleduck.sql.commons.util.SchemaUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

public class SchemaUtilTest {
    @Test
    public void testSchema() throws SQLException, JsonProcessingException {
        DataType.Struct schema = SchemaUtils.buildSchema(
                "select cast(a as int), cast( b as bigint[]), cast(c as struct(a int)), cast(d as decimal) from t " +
                        "where cast(\"x\".\"y\" as varchar) = 'abc' " +
                        "group by cast( g as int)");

        DataType.Struct expected = new DataType.Struct()
        .add("a", DataType.INTEGER)
                .add("b", new DataType.ArrayType(DataType.BIGINT))
                .add("c", new DataType.Struct().add("a", DataType.INTEGER))
                .add("d", new DataType.Decimal(18,3))
                .add("x", new DataType.Struct().add("y", DataType.VARCHAR))
                .add("g", DataType.INTEGER);
        Assertions.assertEquals(expected, schema);
    }

    @Test
    public void testWithNoCast() throws SQLException, JsonProcessingException {
        DataType.Struct schema = SchemaUtils.buildSchema( "select a, b ,c from t");
        DataType.Struct expected = new DataType.Struct()
                .add("a", DataType.NULL)
                .add("b", DataType.NULL)
                .add("c", DataType.NULL);
        Assertions.assertEquals(expected, schema);
    }

    @Test
    public void test() throws SQLException, JsonProcessingException {
        DataType.Struct schema = SchemaUtils.buildSchema( "select count(s.i2), s.i1 from t group by s.i1");
        Assertions.assertEquals(2, ((DataType.Struct)schema.getOrAdd("s", DataType.NULL)).fields.size());
    }
}
