package io.dazzleduck.sql.commons.io;

import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

/**
 * Serializes Arrow query results to a client {@link OutputStream} as Arrow IPC (optionally
 * compressed) or TSV, flushing per batch. Dependency-light: uses only {@code arrow-vector}, with
 * the compression {@link CompressionCodec.Factory} injected by the caller — so this module does
 * not pull {@code arrow-compression}/{@code commons-compress}/{@code zstd-jni}. Callers that want
 * ZSTD/LZ4 pass {@code CommonsCompressionFactory.INSTANCE} (from {@code arrow-compression}).
 *
 * <p>Two layers are exposed:
 * <ul>
 *   <li><b>Pull</b> helpers ({@link #writeArrow}, {@link #writeTsv}) drive an {@link ArrowReader}
 *       to completion — convenient for JDBC/DuckDB callers.</li>
 *   <li><b>Per-batch</b> primitives ({@link #newArrowStreamWriter}, {@link #writeTsvHeader},
 *       {@link #writeTsvRows}, {@link #formatValue}) — for push-based callers (e.g. Flight
 *       listeners) that receive one {@link VectorSchemaRoot} at a time.</li>
 * </ul>
 */
public final class ResultStreams {

    private static final char TAB = '\t';
    private static final char NEWLINE = '\n';

    private ResultStreams() {
    }

    /**
     * Streams every batch of {@code reader} to {@code out} as Arrow IPC, flushing per batch.
     *
     * @param codec   compression codec ({@code NO_COMPRESSION} to disable)
     * @param factory codec implementation factory (ignored when {@code codec} is NO_COMPRESSION)
     * @return total rows written
     */
    public static long writeArrow(ArrowReader reader, OutputStream out,
                                  CompressionUtil.CodecType codec,
                                  CompressionCodec.Factory factory) throws IOException {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        long rows = 0;
        try (ArrowStreamWriter writer = newArrowStreamWriter(root, null, out, codec, factory)) {
            writer.start();
            while (reader.loadNextBatch()) {
                rows += root.getRowCount();
                writer.writeBatch();
                out.flush();
            }
            writer.end();
            out.flush();
        }
        return rows;
    }

    /**
     * Streams every batch of {@code reader} to {@code out} as TSV (header row + tab-separated
     * rows), flushing per batch.
     *
     * @return total rows written
     */
    public static long writeTsv(ArrowReader reader, OutputStream out) throws IOException {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        long rows = 0;
        try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
            writeTsvHeader(root, writer);
            while (reader.loadNextBatch()) {
                writeTsvRows(root, writer);
                rows += root.getRowCount();
                writer.flush();
            }
        }
        return rows;
    }

    /**
     * Creates an {@link ArrowStreamWriter} for {@code root}, with compression when {@code codec}
     * is not {@code NO_COMPRESSION}. The caller supplies the codec factory.
     */
    public static ArrowStreamWriter newArrowStreamWriter(VectorSchemaRoot root,
                                                         DictionaryProvider dictionaries,
                                                         OutputStream out,
                                                         CompressionUtil.CodecType codec,
                                                         CompressionCodec.Factory factory) {
        return newArrowStreamWriter(root, dictionaries, out, codec, factory, IpcOption.DEFAULT);
    }

    /** As above, with an explicit {@link IpcOption} (used by Flight, which carries one). */
    public static ArrowStreamWriter newArrowStreamWriter(VectorSchemaRoot root,
                                                         DictionaryProvider dictionaries,
                                                         OutputStream out,
                                                         CompressionUtil.CodecType codec,
                                                         CompressionCodec.Factory factory,
                                                         IpcOption option) {
        if (codec == null || codec == CompressionUtil.CodecType.NO_COMPRESSION) {
            return new ArrowStreamWriter(root, dictionaries, out);
        }
        return new ArrowStreamWriter(root, dictionaries, Channels.newChannel(out),
                option != null ? option : IpcOption.DEFAULT, factory, codec);
    }

    /** Writes the TSV header row (column names, tab-separated). */
    public static void writeTsvHeader(VectorSchemaRoot root, Writer writer) throws IOException {
        List<FieldVector> vectors = root.getFieldVectors();
        for (int i = 0; i < vectors.size(); i++) {
            if (i > 0) {
                writer.write(TAB);
            }
            writer.write(vectors.get(i).getName());
        }
        writer.write(NEWLINE);
    }

    /** Writes all rows of {@code root} as TSV lines (null cells become empty strings). */
    public static void writeTsvRows(VectorSchemaRoot root, Writer writer) throws IOException {
        List<FieldVector> vectors = root.getFieldVectors();
        int rowCount = root.getRowCount();
        for (int row = 0; row < rowCount; row++) {
            for (int col = 0; col < vectors.size(); col++) {
                if (col > 0) {
                    writer.write(TAB);
                }
                String value = formatValue(vectors.get(col), row);
                if (value != null) {
                    writer.write(value);
                }
            }
            writer.write(NEWLINE);
        }
    }

    /**
     * Formats a single cell as a string. Date/time vectors backed by raw integers are converted to
     * ISO-8601; all other types fall back to {@code getObject().toString()} (readable for numerics,
     * strings, booleans, non-TZ timestamps, lists, structs, maps). Null returns {@code null}.
     */
    public static String formatValue(FieldVector vector, int row) {
        if (vector.isNull(row)) {
            return null;
        }
        return switch (vector.getMinorType()) {
            case DATEDAY ->
                    LocalDate.ofEpochDay(((DateDayVector) vector).get(row)).toString();
            case DATEMILLI ->
                    LocalDate.ofEpochDay(((DateMilliVector) vector).get(row) / 86_400_000L).toString();
            case TIMESEC ->
                    LocalTime.ofSecondOfDay(((TimeSecVector) vector).get(row)).toString();
            case TIMEMILLI ->
                    LocalTime.ofNanoOfDay((long) ((TimeMilliVector) vector).get(row) * 1_000_000L).toString();
            case TIMEMICRO ->
                    LocalTime.ofNanoOfDay(((TimeMicroVector) vector).get(row) * 1_000L).toString();
            case TIMENANO ->
                    LocalTime.ofNanoOfDay(((TimeNanoVector) vector).get(row)).toString();
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
            default -> {
                Object value = vector.getObject(row);
                yield value != null ? value.toString() : null;
            }
        };
    }
}
