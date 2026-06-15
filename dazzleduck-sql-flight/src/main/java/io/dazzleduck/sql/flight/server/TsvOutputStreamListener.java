package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.io.ResultStreams;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
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
        ResultStreams.writeTsvHeader(root, writer);
    }

    private void writeRows() throws IOException {
        ResultStreams.writeTsvRows(root, writer);
    }

    /**
     * Pipes Arrow IPC bytes from {@code arrowStreamFn} through an {@link ArrowStreamReader}
     * and writes TSV output to {@code outputStreamSupplier}. Reusable by any HTTP service
     * that needs Arrow-IPC-to-TSV conversion without holding a shared allocator.
     */
    public static CompletableFuture<Void> pipeArrowToTsv(
            Function<Supplier<OutputStream>, CompletableFuture<Void>> arrowStreamFn,
            Supplier<OutputStream> outputStreamSupplier) {
        CompletableFuture<Void> tsvFuture = new CompletableFuture<>();
        try {
            PipedOutputStream pipe = new PipedOutputStream();
            PipedInputStream pipeIn = new PipedInputStream(pipe, 1 << 20);
            Thread.ofVirtual().start(() -> {
                try (RootAllocator allocator = new RootAllocator();
                     ArrowStreamReader reader = new ArrowStreamReader(pipeIn, allocator)) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    TsvOutputStreamListener listener =
                            new TsvOutputStreamListener(outputStreamSupplier, tsvFuture);
                    listener.start(root, new DictionaryProvider.MapDictionaryProvider(), IpcOption.DEFAULT);
                    while (reader.loadNextBatch()) listener.putNext();
                    listener.completed();
                } catch (Exception e) {
                    try { pipeIn.close(); } catch (IOException ignored) {}
                    if (!tsvFuture.isDone()) tsvFuture.completeExceptionally(e);
                }
            });
            arrowStreamFn.apply(() -> pipe).exceptionally(e -> {
                try { pipe.close(); } catch (IOException ignored) {}
                if (!tsvFuture.isDone()) tsvFuture.completeExceptionally(e);
                return null;
            });
        } catch (Exception e) {
            tsvFuture.completeExceptionally(e);
        }
        return tsvFuture;
    }

    /** @deprecated use {@link ResultStreams#writeTsvRows(VectorSchemaRoot, Writer)}. */
    @Deprecated
    public static void writeRootToWriter(VectorSchemaRoot root, Writer writer) throws IOException {
        ResultStreams.writeTsvRows(root, writer);
    }
}
