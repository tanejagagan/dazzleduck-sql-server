package io.dazzleduck.sql.flight.server;

import com.fasterxml.jackson.core.JsonGenerator;
import io.dazzleduck.sql.commons.io.ResultStreams;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A ServerStreamListener that writes Arrow batches as JSON to an OutputStream.
 *
 * <p>Supports three {@link Format}s:
 * <ul>
 *   <li>{@link Format#ARRAY} — a single JSON array of row objects (default).</li>
 *   <li>{@link Format#SINGLE_OBJECT} — the first row only, as a bare object
 *       (errors with {@link NoSuchElementException} if there are no rows).</li>
 *   <li>{@link Format#JSONL} — JSON Lines / NDJSON: one row object per line,
 *       newline-terminated, with no enclosing array.</li>
 * </ul>
 *
 * <p>Each row is written by {@link ResultStreams#writeJsonRow}, which also backs
 * {@link ResultStreams#writeJsonl} for non-Flight callers, so value formatting is shared.
 */
public class JsonOutputStreamListener implements FlightProducer.ServerStreamListener {

    /** Output shape written by this listener. */
    public enum Format {
        /** One JSON array of row objects. */
        ARRAY,
        /** Only the first row, as a bare JSON object. */
        SINGLE_OBJECT,
        /** JSON Lines / NDJSON: one newline-terminated object per row. */
        JSONL
    }

    private static final Logger logger = LoggerFactory.getLogger(JsonOutputStreamListener.class);

    private final Supplier<OutputStream> outputStreamSupplier;
    private final CompletableFuture<Void> future;
    private final Format format;

    private OutputStream outputStream;
    private JsonGenerator generator;
    private VectorSchemaRoot root;
    private boolean firstRowWritten = false;

    public JsonOutputStreamListener(Supplier<OutputStream> outputStreamSupplier, CompletableFuture<Void> future) {
        this(outputStreamSupplier, future, Format.ARRAY);
    }

    public JsonOutputStreamListener(Supplier<OutputStream> outputStreamSupplier, CompletableFuture<Void> future, boolean includeArrayBrackets) {
        this(outputStreamSupplier, future, includeArrayBrackets ? Format.ARRAY : Format.SINGLE_OBJECT);
    }

    public JsonOutputStreamListener(Supplier<OutputStream> outputStreamSupplier, CompletableFuture<Void> future, Format format) {
        this.outputStreamSupplier = outputStreamSupplier;
        this.future = future;
        this.format = format;
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
        // We are ready if the future is not complete
        return !future.isDone();
    }

    @Override
    public synchronized void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
        this.root = root;
        try {
            // ARRAY and JSONL commit the response eagerly so an empty result still
            // produces a valid body ("[]" / empty stream). SINGLE_OBJECT defers until
            // the first row so a missing row can surface as an error status.
            if (format == Format.ARRAY || format == Format.JSONL) {
                ensureGenerator();
            }
            logger.debug("JsonOutputStreamListener started with schema: {}, format: {}",
                    root.getSchema(), format);
        } catch (Exception e) {
            logger.error("Error in start()", e);
            future.completeExceptionally(e);
        }
    }

    private void ensureGenerator() throws IOException {
        if (generator == null) {
            this.outputStream = outputStreamSupplier.get();
            this.generator = format == Format.JSONL
                    ? ResultStreams.newJsonlGenerator(outputStream)
                    : ResultStreams.newJsonGenerator(outputStream);
            if (format == Format.ARRAY) {
                generator.writeStartArray();
            }
        }
    }

    @Override
    public synchronized void putNext() {
        try {
            ensureGenerator();
            writeRows();
            generator.flush();
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
            if (generator != null) {
                generator.close();
            } else if (outputStream != null) {
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
            if (!firstRowWritten && format == Format.SINGLE_OBJECT) {
                throw new NoSuchElementException("No rows found");
            }
            if (generator != null) {
                if (format == Format.ARRAY) {
                    generator.writeEndArray();
                }
                generator.flush();
                generator.close();
            }
            future.complete(null);
        } catch (Exception e) {
            if (!(e instanceof NoSuchElementException)) {
                logger.error("Error in completed()", e);
            }
            future.completeExceptionally(e);
        }
    }

    private void writeRows() throws IOException {
        int rowCount = root.getRowCount();
        for (int row = 0; row < rowCount; row++) {
            if (format == Format.SINGLE_OBJECT && firstRowWritten) {
                break; // Only write the first row in single-object mode
            }
            ResultStreams.writeJsonRow(root, row, generator);
            if (format == Format.JSONL) {
                generator.writeRaw('\n'); // newline-delimit each row object
            }
            firstRowWritten = true;
        }
    }
}
