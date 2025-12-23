package io.dazzleduck.sql.flight.stream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Objects;

public class ArrowStreamReaderWrapper extends ArrowStreamReader {
    private final ArrowReader arrowReader;

    public ArrowStreamReaderWrapper(ArrowReader reader, BufferAllocator allocator) {
        super(new ByteArrayInputStream(new byte[0]), allocator);
        this.arrowReader = Objects.requireNonNull(reader, "reader must not be null");
    }

    @Override
    protected Schema readSchema() throws IOException {
        return arrowReader.getVectorSchemaRoot().getSchema();
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot() throws IOException {
        return arrowReader.getVectorSchemaRoot();
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        return arrowReader.loadNextBatch();
    }

    @Override
    public long bytesRead() {
        return arrowReader.bytesRead();
    }

    @Override
    public void close() throws IOException {
        try {
            arrowReader.close();
        } finally {
            super.close();
        }
    }
}
