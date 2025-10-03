package io.dazzleduck.sql.commons.types;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.shaded.org.apache.commons.lang3.NotImplementedException;

public class VectorSchemaRootWriter {

    @SuppressWarnings("rawtypes")
    private final VectorWriter[] functions;
    private final Schema schema;

    public VectorSchemaRootWriter(Schema schema,
                                  @SuppressWarnings("rawtypes") VectorWriter... functions) {
        this.functions = functions;
        this.schema = schema;
    }

    public VectorSchemaRoot writeToVector(JavaRow[] rows, VectorSchemaRoot root) {
        root.allocateNew();
        for (int i = 0; i < rows.length; i++) {
            for (int j = 0; j < functions.length; j++) {
                var function = functions[j];
                var vector = root.getVector(j);
                //noinspection unchecked
                function.write(vector, i, rows[i].get(j));
            }
        }
        root.setRowCount(rows.length);
        return root;
    }

    //TODO
    public static VectorSchemaRootWriter of(Schema schema) {
        throw new NotImplementedException("Need to provide an implementation");
    }
}
