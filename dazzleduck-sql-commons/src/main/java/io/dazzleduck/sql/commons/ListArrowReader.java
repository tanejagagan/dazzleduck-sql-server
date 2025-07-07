package io.dazzleduck.sql.commons;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ListArrowReader extends ArrowReader {
    private final Schema schema;
    private final List<ArrowRecordBatch> batches;
    int nextIndex;

    VectorSchemaRoot root;
    public ListArrowReader(BufferAllocator allocator,
                           Schema schema,
                           List<ArrowRecordBatch> batches) {
        super(allocator);
        this.schema = schema;
        this.batches = batches;
        this.nextIndex = 0;
        this.root = VectorSchemaRoot.create(schema, allocator);
    }

    public static ArrowReader readAllData(BufferAllocator allocator, ArrowReader reader) throws IOException {
        List<ArrowRecordBatch>  batches = new ArrayList<>();
        Schema schema = reader.getVectorSchemaRoot().getSchema();
        while (reader.loadNextBatch()) {
            VectorUnloader unloader = new VectorUnloader(reader.getVectorSchemaRoot());
            batches.add(unloader.getRecordBatch());
        }
        return new ListArrowReader(allocator, schema, batches);
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        if (nextIndex < batches.size()) {
            new VectorLoader(root).load(batches.get(nextIndex++));
            return true;
        }
        return false;
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot() {
        return root;
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {

    }

    @Override
    protected Schema readSchema() {
        return schema;
    }

    @Override
    public synchronized void close() throws IOException {
        root.close();
        try {
            AutoCloseables.close(batches);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
