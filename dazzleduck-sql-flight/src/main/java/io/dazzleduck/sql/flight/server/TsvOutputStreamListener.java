package io.dazzleduck.sql.flight.server;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A ServerStreamListener that writes Arrow batches as TSV (tab-separated values) to an OutputStream.
 *
 * <p>The first batch triggers writing the header row (column names). Each subsequent call to
 * {@link #putNext()} writes all rows from the current {@link VectorSchemaRoot} as TSV lines.
 * Null values are written as empty strings.
 */
public class TsvOutputStreamListener implements FlightProducer.ServerStreamListener {

    private static final Logger logger = LoggerFactory.getLogger(TsvOutputStreamListener.class);
    private static final char TAB = '\t';
    private static final char NEWLINE = '\n';

    private final Supplier<OutputStream> outputStreamSupplier;
    private final CompletableFuture<Void> future;
    private OutputStream outputStream;
    private Writer writer;
    private VectorSchemaRoot root;
    private boolean headerWritten = false;

    public TsvOutputStreamListener(Supplier<OutputStream> outputStreamSupplier, CompletableFuture<Void> future) {
        this.outputStreamSupplier = outputStreamSupplier;
        this.future = future;
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public void setOnCancelHandler(Runnable handler) {
        // No-op for HTTP streaming
    }

    @Override
    public boolean isReady() {
        return writer != null;
    }

    @Override
    public synchronized void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
        try {
            this.root = root;
            this.outputStream = outputStreamSupplier.get();
            this.writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
            logger.debug("TsvOutputStreamListener started with schema: {}", root.getSchema());
        } catch (Exception e) {
            logger.error("Error in start()", e);
            future.completeExceptionally(e);
        }
    }

    @Override
    public synchronized void putNext() {
        try {
            if (!headerWritten) {
                writeHeader();
                headerWritten = true;
            }
            writeRows();
            writer.flush();
        } catch (IOException e) {
            logger.error("Error in putNext()", e);
            future.completeExceptionally(e);
        }
    }

    @Override
    public synchronized void putNext(ArrowBuf metadata) {
        putNext();
    }

    @Override
    public synchronized void putMetadata(ArrowBuf metadata) {
        // No-op
    }

    @Override
    public synchronized void error(Throwable ex) {
        try {
            if (outputStream != null) {
                outputStream.close();
            }
        } catch (Exception ignored) {
        } finally {
            future.completeExceptionally(ex);
        }
    }

    @Override
    public synchronized void completed() {
        try {
            if (!headerWritten && root != null) {
                writeHeader();
                headerWritten = true;
            }
            if (writer != null) {
                writer.flush();
                writer.close();
            }
            future.complete(null);
        } catch (Exception e) {
            logger.error("Error in completed()", e);
            future.completeExceptionally(e);
        }
    }

    private void writeHeader() throws IOException {
        List<FieldVector> vectors = root.getFieldVectors();
        for (int i = 0; i < vectors.size(); i++) {
            if (i > 0) writer.write(TAB);
            writer.write(vectors.get(i).getName());
        }
        writer.write(NEWLINE);
    }

    private void writeRows() throws IOException {
        List<FieldVector> vectors = root.getFieldVectors();
        int rowCount = root.getRowCount();
        for (int row = 0; row < rowCount; row++) {
            for (int col = 0; col < vectors.size(); col++) {
                if (col > 0) writer.write(TAB);
                Object value = vectors.get(col).getObject(row);
                if (value != null) {
                    writer.write(value.toString());
                }
            }
            writer.write(NEWLINE);
        }
    }
}
