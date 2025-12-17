package io.dazzleduck.sql.common.ingestion;

import io.dazzleduck.sql.common.types.JavaRow;
import io.dazzleduck.sql.common.types.VectorSchemaRootWriter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public final class Bucket {

    private final Schema schema;
    private final BufferAllocator allocator;

    private final List<JavaRow> buffer = new ArrayList<>();
    private long size = 0;
    private boolean serialized = false;

    public Bucket(Schema schema) {
        this.schema = schema;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    public synchronized long add(JavaRow row) {
        if(serialized) {
            throw new IllegalStateException("the bucket is already serialized");
        }
        buffer.add(row);
        size += row.getActualSize();
        return size;
    }

    public synchronized long size() {
        return size;
    }

    public synchronized byte[] getArrowBytes() {
        if (serialized || buffer.isEmpty()) {
            return null;
        }
        serialized = true;

        try (
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)
        ) {
            JavaRow[] rows = buffer.toArray(JavaRow[]::new);
            VectorSchemaRootWriter.of(schema).writeToVector(rows, root);
            root.setRowCount(rows.length);

            writer.start();
            writer.writeBatch();
            writer.end();

            return out.toByteArray();

        } catch (Exception e) {
            throw new RuntimeException("Arrow serialization failed", e);
        } finally {
            buffer.clear();
            allocator.close();
        }
    }
}
