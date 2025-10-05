package io.dazzleduck.sql.commons.types;


import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;
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
                "one",
                List.of(1, 2, 3),
                List.of("one", "two"),
                Map.of("one", 1, "two", 2)
        });
        var row2 = new JavaRow(new Object[]{
                2,
                "two",
                List.of(7, 8, 9),
                List.of("four", "three"),
                Map.of("six", 6, "seven", 7)
        });
        JavaRow[] testRows = {row1, row2};

        try (var allocator = new RootAllocator()) {
            var intVector = new IntVector("int", allocator);
            var varcharVector = new VarCharVector("varchar", allocator);
            ListVector listVector = ListVector.empty("myIntList", allocator);
            ListVector varcharListVector = ListVector.empty("myCharVector", allocator);
            MapVector mapVector = MapVector.empty("myMap", allocator, false);
            var intWriteFunction = new VectorWriter.IntVectorWriter();
            var varcharWriteFunction = new VectorWriter.VarCharVectorWriter();
            var intListWriteFunction = new VectorWriter.ListVectorWriter(ElementWriteFunction.INT);
            var varcharListWriteFunction = new VectorWriter.ListVectorWriter(ElementWriteFunction.VARCHAR);
            var mapWriteFunction = new VectorWriter.MapVectorWriter(ElementWriteFunction.VARCHAR, ElementWriteFunction.INT);
            Schema schema = null;
            var vectorSchemaRootWriter = new VectorSchemaRootWriter(schema, intWriteFunction,
                    varcharWriteFunction,
                    intListWriteFunction,
                    varcharListWriteFunction,
                    mapWriteFunction);
            try (var root = VectorSchemaRoot.of(intVector, varcharVector, listVector, varcharListVector, mapVector)) {
                vectorSchemaRootWriter.writeToVector(testRows, root);
                System.out.println(root.contentToTSVString());
            }
        }
    }
}
