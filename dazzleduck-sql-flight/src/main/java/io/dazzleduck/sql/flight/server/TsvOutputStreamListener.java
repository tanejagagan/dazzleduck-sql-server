package io.dazzleduck.sql.flight.server;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
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
                String value = formatValue(vectors.get(col), row);
                if (value != null) {
                    writer.write(value);
                }
            }
            writer.write(NEWLINE);
        }
    }

    /**
     * Formats a single cell value as a string.
     *
     * Arrow vectors for date/time types backed by raw integers are converted to
     * ISO-8601 strings. All other types fall back to {@code getObject().toString()},
     * which already produces readable output for numerics, strings, booleans,
     * non-TZ timestamps (LocalDateTime), lists (JsonStringArrayList), and
     * structs (JsonStringHashMap).
     */
    private String formatValue(FieldVector vector, int row) {
        if (vector.isNull(row)) return null;
        return switch (vector.getMinorType()) {
            // Date types — getObject() returns Integer (days since epoch)
            case DATEDAY ->
                LocalDate.ofEpochDay(((DateDayVector) vector).get(row)).toString();
            case DATEMILLI ->
                LocalDate.ofEpochDay(((DateMilliVector) vector).get(row) / 86_400_000L).toString();

            // Time types — getObject() returns raw Integer/Long
            case TIMESEC ->
                LocalTime.ofSecondOfDay(((TimeSecVector) vector).get(row)).toString();
            case TIMEMILLI ->
                LocalTime.ofNanoOfDay((long) ((TimeMilliVector) vector).get(row) * 1_000_000L).toString();
            case TIMEMICRO ->
                LocalTime.ofNanoOfDay(((TimeMicroVector) vector).get(row) * 1_000L).toString();
            case TIMENANO ->
                LocalTime.ofNanoOfDay(((TimeNanoVector) vector).get(row)).toString();

            // Timezone-aware timestamp types — getObject() returns Long (raw units since epoch)
            case TIMESTAMPSECTZ ->
                Instant.ofEpochSecond(((TimeStampSecTZVector) vector).get(row)).toString();
            case TIMESTAMPMILLITZ ->
                Instant.ofEpochMilli(((TimeStampMilliTZVector) vector).get(row)).toString();
            case TIMESTAMPMICROTZ -> {
                long micros = ((TimeStampMicroTZVector) vector).get(row);
                yield Instant.ofEpochSecond(
                        Math.floorDiv(micros, 1_000_000L),
                        Math.floorMod(micros, 1_000_000L) * 1_000L).toString();
            }
            case TIMESTAMPNANOTZ -> {
                long nanos = ((TimeStampNanoTZVector) vector).get(row);
                yield Instant.ofEpochSecond(
                        Math.floorDiv(nanos, 1_000_000_000L),
                        Math.floorMod(nanos, 1_000_000_000L)).toString();
            }

            // All other types (numerics, strings, booleans, non-TZ timestamps,
            // lists, structs, maps) — getObject() already returns a readable value
            default -> {
                Object value = vector.getObject(row);
                yield value != null ? value.toString() : null;
            }
        };
    }
}
