package io.dazzleduck.sql.commons.types;


import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.apache.arrow.vector.types.pojo.Field;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

public class VectorSchemaRootWriteTest {

    @Test
    public void testList() {
        var row1 = new JavaRow(new Object[]{
                List.of(1, 2, 3),
                List.of("one", "two")
        });
        var row2 = new JavaRow(new Object[]{
                List.of(7, 8, 9),
                List.of("four", "three")
        });
        var intListWriteFunction = new VectorWriter.ListVectorWriter(ElementWriteFunction.INT);
        var varcharListWriteFunction = new VectorWriter.ListVectorWriter(ElementWriteFunction.VARCHAR);
        JavaRow[] testRows = {row1, row2};
        Schema schema = null;
        var vectorSchemaRootWriter = new VectorSchemaRootWriter(schema, intListWriteFunction, varcharListWriteFunction);
        try (var allocator = new RootAllocator()) {
            ListVector listVector = ListVector.empty("myIntList", allocator);
            ListVector varcharListVector = ListVector.empty("myCharVector", allocator);
            try (var root = VectorSchemaRoot.of(listVector, varcharListVector)) {
                vectorSchemaRootWriter.writeToVector(testRows, root);
                System.out.println(root.contentToTSVString());
            }
        }
    }

    @Test
    public void testMap() {
        var row1 = new JavaRow(new Object[]{Map.of("one", 1, "two", 2)});
        var row2 = new JavaRow(new Object[]{Map.of("six", 6, "seven", 7)});
        JavaRow[] testRows = {row1, row2};
        var mapWriteFunction = new VectorWriter.MapVectorWriter(ElementWriteFunction.VARCHAR, ElementWriteFunction.INT);
        Schema schema = null;
        var vectorSchemaRootWriter = new VectorSchemaRootWriter(schema, mapWriteFunction);
        try (var allocator = new RootAllocator()) {
            MapVector mapVector = MapVector.empty("myMap", allocator, false);
            try (var root = VectorSchemaRoot.of(mapVector)) {
                vectorSchemaRootWriter.writeToVector(testRows, root);
                System.out.println(root.contentToTSVString());
            }
        }
    }

    @Test
    public void testSimple() {
        var row1 = new JavaRow(new Object[]{
                1,
                12L,
                12.01,
                "one",
                List.of(1, 2, 3),
                List.of("one", "two"),
                Map.of("one", 1, "two", 2)
        });
        var row2 = new JavaRow(new Object[]{
                2,
                121L,
                1.01,
                "two",
                List.of(7, 8, 9),
                List.of("four", "three"),
                Map.of("six", 6, "seven", 7)
        });
        JavaRow[] testRows = {row1, row2};

        try (var allocator = new RootAllocator()) {
            var intVector = new IntVector("int", allocator);
            var bigIntVector = new BigIntVector("bigInt", allocator);
            var floatVector = new Float8Vector("float", allocator);
            var varcharVector = new VarCharVector("varchar", allocator);
            ListVector listVector = ListVector.empty("myIntList", allocator);
            ListVector varcharListVector = ListVector.empty("myCharVector", allocator);
            MapVector mapVector = MapVector.empty("myMap", allocator, false);
            var intWriteFunction = new VectorWriter.IntVectorWriter();
            var bigWriteFunction = new VectorWriter.BigVectorWriter();
            var floatWriteFunction = new VectorWriter.FloatVectorWriter();
            var varcharWriteFunction = new VectorWriter.VarCharVectorWriter();
            var intListWriteFunction = new VectorWriter.ListVectorWriter(ElementWriteFunction.INT);
            var varcharListWriteFunction = new VectorWriter.ListVectorWriter(ElementWriteFunction.VARCHAR);
            var mapWriteFunction = new VectorWriter.MapVectorWriter(ElementWriteFunction.VARCHAR, ElementWriteFunction.INT);
            Schema schema = null;
            var vectorSchemaRootWriter = new VectorSchemaRootWriter(schema, intWriteFunction,
                    bigWriteFunction,
                    floatWriteFunction,
                    varcharWriteFunction,
                    intListWriteFunction,
                    varcharListWriteFunction,
                    mapWriteFunction);
            try (var root = VectorSchemaRoot.of(intVector, bigIntVector, floatVector, varcharVector, listVector, varcharListVector, mapVector)) {
                vectorSchemaRootWriter.writeToVector(testRows, root);
                System.out.println(root.contentToTSVString());
            }
        }
    }

    @Test
    public void testOfFactory() {
        // --- Build schema with primitive, list, and map ---
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);

        // List<Int> field
        FieldType intType = new FieldType(true, new ArrowType.Int(32, true), null);
        Field listChild = new Field("intCol", intType, null);
        Field points = new Field("points", FieldType.nullable(new ArrowType.List()), Collections.singletonList(listChild));

        // Map<Utf8, Int> field
        Field keyField = new Field("key", FieldType.nullable(new ArrowType.Utf8()), null);
        Field valueField = new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field mapStruct = new Field("entries", FieldType.nullable(new ArrowType.Struct()), asList(keyField, valueField));
        Field mapField = new Field("scores", FieldType.nullable(new ArrowType.Map(false)), Collections.singletonList(mapStruct));
        Field dateField = new Field("date", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), null);
        Schema schema = new Schema(asList(name, age, points, mapField, dateField));
        // --- Create rows ---
        JavaRow row1 = new JavaRow(new Object[]{
                "John", 25,
                List.of(10, 20, 30),
                Map.of("math", 90, "english", 85),
                Instant.now().toEpochMilli()
        });

        JavaRow row2 = new JavaRow(new Object[]{
                "David", 30,
                List.of(40, 50),
                Map.of("math", 75, "english", 95),
                Instant.now().toEpochMilli()
        });

        JavaRow[] rows = {row1, row2};

        // --- Create VectorSchemaRoot ---
        try (var allocator = new RootAllocator()) {
            var nameVector = new VarCharVector("name", allocator);
            var ageVector = new IntVector("age", allocator);
            var pointsVector = ListVector.empty("points", allocator);
            var scoresVector = MapVector.empty("scores", allocator, false);
            var dateVector = new TimeStampMilliVector("date", allocator);

            try (VectorSchemaRoot root = VectorSchemaRoot.of(nameVector, ageVector, pointsVector, scoresVector, dateVector)) {
                // --- Use the factory ---
                VectorSchemaRootWriter writer = VectorSchemaRootWriter.of(schema);
                // --- Write rows ---
                writer.writeToVector(rows, root);

                VarCharVector nameVectorActual = (VarCharVector) root.getVector("name");
                List<String> actual = new ArrayList<>();
                for (int i = 0; i < nameVectorActual.getValueCount(); i++) {
                    if (!nameVectorActual.isNull(i)) {
                        actual.add(nameVectorActual.getObject(i).toString());
                    }
                }

                // --- Assertions ---
                assertEquals(List.of("John", "David"), actual);
                assertEquals(2, root.getRowCount());
            }
        }
    }
}
