package io.dazzleduck.sql.commons.io;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;

import java.io.IOException;
import java.math.BigDecimal;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

/**
 * Serializes Arrow query results to a client {@link OutputStream} as Arrow IPC (optionally
 * compressed) or TSV, flushing per batch. Dependency-light: uses only {@code arrow-vector}, with
 * the compression {@link CompressionCodec.Factory} injected by the caller — so this module does
 * not pull {@code arrow-compression}/{@code commons-compress}/{@code zstd-jni}. Callers that want
 * ZSTD/LZ4 pass {@code CommonsCompressionFactory.INSTANCE} (from {@code arrow-compression}).
 *
 * <p>Two layers are exposed:
 * <ul>
 *   <li><b>Pull</b> helpers ({@link #writeArrow}, {@link #writeTsv}, {@link #writeJsonl}) drive an
 *       {@link ArrowReader} to completion — convenient for JDBC/DuckDB callers.</li>
 *   <li><b>Per-batch</b> primitives ({@link #newArrowStreamWriter}, {@link #writeTsvHeader},
 *       {@link #writeTsvRows}, {@link #formatValue}, {@link #writeJsonRow}) — for push-based callers
 *       (e.g. Flight listeners) that receive one {@link VectorSchemaRoot} at a time.</li>
 * </ul>
 *
 * <p>Dictionary-encoded columns (DuckDB sends {@code ENUM} this way, also inside lists and structs)
 * need the stream's {@link DictionaryProvider}: an {@link ArrowReader} is one. The per-batch
 * primitives take it as a parameter; their overloads without it are for results known to have no
 * dictionary-encoded column, and fail on one rather than print its dictionary indices. Whether a
 * column needs its dictionaries is decided once per column from the schema, so columns without one
 * cost nothing extra. A dictionary-encoded value can be resolved inside lists (large ones too),
 * maps, fixed-size lists and structs; inside any other type (e.g. a union, or a list view) the
 * column fails before a row is written.
 */
public final class ResultStreams {

    private static final char TAB = '\t';
    private static final char NEWLINE = '\n';

    /** Passed down for a column with no dictionary-encoded value anywhere inside: no lookups. */
    private static final DictionaryProvider NO_DICTIONARIES = new DictionaryProvider.MapDictionaryProvider();

    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    // JavaTimeModule + ISO output so java.time values (e.g. non-TZ TIMESTAMP -> LocalDateTime),
    // including those nested inside structs/lists, serialize as strings rather than failing.
    // FLUSH_AFTER_WRITE_VALUE is off so writing a value does not flush the generator and the
    // underlying stream (one HTTP chunk per value); callers flush per batch.
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(SerializationFeature.FLUSH_AFTER_WRITE_VALUE);

    private ResultStreams() {
    }

    /**
     * Streams every batch of {@code reader} to {@code out} as Arrow IPC, flushing per batch.
     * Closes {@code out} when done.
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
        try (ArrowStreamWriter writer = newArrowStreamWriter(root, reader, out, codec, factory)) {
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
     * rows), flushing per batch. Closes {@code out} when done.
     *
     * @return total rows written
     */
    public static long writeTsv(ArrowReader reader, OutputStream out) throws IOException {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        long rows = 0;
        try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
            writeTsvHeader(root, writer);
            while (reader.loadNextBatch()) {
                writeTsvRows(root, reader, writer);
                rows += root.getRowCount();
                writer.flush();
            }
        }
        return rows;
    }

    /**
     * Streams every batch of {@code reader} to {@code out} as JSON Lines (NDJSON): one JSON object
     * per row, newline-terminated, no enclosing array, flushing per batch. An empty result writes
     * nothing. Values are typed as in {@link #writeJsonRow}. Closes {@code out} when done.
     *
     * @return total rows written
     */
    public static long writeJsonl(ArrowReader reader, OutputStream out) throws IOException {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        long rows = 0;
        try (JsonGenerator generator = newJsonlGenerator(out)) {
            while (reader.loadNextBatch()) {
                int rowCount = root.getRowCount();
                for (int row = 0; row < rowCount; row++) {
                    writeJsonRow(root, reader, row, generator);
                    generator.writeRaw(NEWLINE);
                }
                rows += rowCount;
                generator.flush();
            }
        }
        return rows;
    }

    /** A generator for JSON output to {@code out}; the caller writes values and closes it. */
    public static JsonGenerator newJsonGenerator(OutputStream out) throws IOException {
        return JSON_FACTORY.createGenerator(out);
    }

    /**
     * A generator for JSON Lines: the default single-space separator between root-level values is
     * suppressed, since each row object is followed by its own newline instead.
     */
    public static JsonGenerator newJsonlGenerator(OutputStream out) throws IOException {
        JsonGenerator generator = newJsonGenerator(out);
        generator.setRootValueSeparator(new SerializedString(""));
        return generator;
    }

    /**
     * Writes row {@code row} of {@code root} as one JSON object, keyed by column name. Numbers
     * (including decimals and unsigned integers) and booleans keep their JSON types; dates, times
     * and timestamps are ISO-8601 strings; binary is base64; lists are arrays, structs are objects,
     * and maps are objects keyed by the map key's text; nulls are JSON null. The same rules apply
     * at every nesting level, so a DATE inside a struct renders like a top-level one.
     */
    public static void writeJsonRow(VectorSchemaRoot root, int row, JsonGenerator generator) throws IOException {
        writeJsonRow(root, null, row, generator);
    }

    /**
     * As {@link #writeJsonRow(VectorSchemaRoot, int, JsonGenerator)}, writing a dictionary-encoded
     * value (at any depth) as the dictionary entry it refers to, looked up in {@code dictionaries}.
     */
    public static void writeJsonRow(VectorSchemaRoot root, DictionaryProvider dictionaries, int row,
                                    JsonGenerator generator) throws IOException {
        List<FieldVector> vectors = root.getFieldVectors();
        List<Field> fields = root.getSchema().getFields();
        generator.writeStartObject();
        for (int col = 0; col < vectors.size(); col++) {
            FieldVector vector = vectors.get(col);
            generator.writeFieldName(vector.getName());
            writeJsonValue(vector, row, generator, forColumn(fields.get(col), dictionaries));
        }
        generator.writeEndObject();
    }

    /**
     * Writes the value at {@code index} of {@code vector}. Lists, structs and maps are walked through
     * their child vectors, so nested values get the same formatting as top-level ones.
     */
    private static void writeJsonValue(ValueVector vector, int index, JsonGenerator generator,
                                       DictionaryProvider dictionaries) throws IOException {
        if (vector.isNull(index)) {
            generator.writeNull();
            return;
        }
        Dictionary dictionary = dictionaries == NO_DICTIONARIES ? null : dictionaryOf(vector, dictionaries);
        if (dictionary != null) {
            writeJsonValue(dictionary.getVector(), dictionaryIndex(vector, index), generator, dictionaries);
            return;
        }
        switch (vector.getMinorType()) {
            case TINYINT -> generator.writeNumber(((TinyIntVector) vector).get(index));
            case SMALLINT -> generator.writeNumber(((SmallIntVector) vector).get(index));
            case INT -> generator.writeNumber(((IntVector) vector).get(index));
            case BIGINT -> generator.writeNumber(((BigIntVector) vector).get(index));
            case UINT1 -> generator.writeNumber(((UInt1Vector) vector).getObjectNoOverflow(index));
            case UINT2 -> generator.writeNumber((int) ((UInt2Vector) vector).get(index));
            case UINT4 -> generator.writeNumber(((UInt4Vector) vector).getObjectNoOverflow(index));
            case UINT8 -> generator.writeNumber(((UInt8Vector) vector).getObjectNoOverflow(index));
            case FLOAT4 -> generator.writeNumber(((Float4Vector) vector).get(index));
            case FLOAT8 -> generator.writeNumber(((Float8Vector) vector).get(index));
            case DECIMAL, DECIMAL256 -> generator.writeNumber((BigDecimal) vector.getObject(index));
            case BIT -> generator.writeBoolean(((BitVector) vector).get(index) != 0);
            case VARCHAR -> generator.writeString(((VarCharVector) vector).getObject(index).toString());
            case VARBINARY -> generator.writeBinary(((VarBinaryVector) vector).get(index));
            // Dates, times and timestamps (with or without zone) share formatValue's ISO-8601 text.
            case DATEDAY, DATEMILLI, TIMESEC, TIMEMILLI, TIMEMICRO, TIMENANO,
                 TIMESTAMPSEC, TIMESTAMPMILLI, TIMESTAMPMICRO, TIMESTAMPNANO,
                 TIMESTAMPSECTZ, TIMESTAMPMILLITZ, TIMESTAMPMICROTZ, TIMESTAMPNANOTZ ->
                    generator.writeString(format((FieldVector) vector, index, NO_DICTIONARIES));
            case MAP -> {
                // MapVector is a ListVector of {key, value} structs; render it as a JSON object.
                MapVector map = (MapVector) vector;
                StructVector entries = (StructVector) map.getDataVector();
                FieldVector keys = (FieldVector) entries.getChildrenFromFields().get(0);
                ValueVector values = entries.getChildrenFromFields().get(1);
                generator.writeStartObject();
                for (int i = map.getElementStartIndex(index); i < map.getElementEndIndex(index); i++) {
                    generator.writeFieldName(format(keys, i, dictionaries));
                    writeJsonValue(values, i, generator, dictionaries);
                }
                generator.writeEndObject();
            }
            case LIST -> {
                ListVector list = (ListVector) vector;
                ValueVector elements = list.getDataVector();
                generator.writeStartArray();
                for (int i = list.getElementStartIndex(index); i < list.getElementEndIndex(index); i++) {
                    writeJsonValue(elements, i, generator, dictionaries);
                }
                generator.writeEndArray();
            }
            case LARGELIST -> {
                LargeListVector list = (LargeListVector) vector;
                ValueVector elements = list.getDataVector();
                generator.writeStartArray();
                for (long i = list.getElementStartIndex(index); i < list.getElementEndIndex(index); i++) {
                    writeJsonValue(elements, Math.toIntExact(i), generator, dictionaries);
                }
                generator.writeEndArray();
            }
            case FIXED_SIZE_LIST -> {
                FixedSizeListVector list = (FixedSizeListVector) vector;
                ValueVector elements = list.getDataVector();
                int size = list.getListSize();
                generator.writeStartArray();
                for (int i = index * size; i < (index + 1) * size; i++) {
                    writeJsonValue(elements, i, generator, dictionaries);
                }
                generator.writeEndArray();
            }
            case STRUCT -> {
                StructVector struct = (StructVector) vector;
                generator.writeStartObject();
                for (FieldVector child : struct.getChildrenFromFields()) {
                    generator.writeFieldName(child.getName());
                    writeJsonValue(child, index, generator, dictionaries);
                }
                generator.writeEndObject();
            }
            // Anything else (e.g. unions): Jackson on the Java object.
            default -> MAPPER.writeValue(generator, vector.getObject(index));
        }
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
        writeTsvRows(root, null, writer);
    }

    /**
     * As {@link #writeTsvRows(VectorSchemaRoot, Writer)}, writing a dictionary-encoded value (at any
     * depth) as the dictionary entry it refers to, looked up in {@code dictionaries}.
     */
    public static void writeTsvRows(VectorSchemaRoot root, DictionaryProvider dictionaries, Writer writer)
            throws IOException {
        List<FieldVector> vectors = root.getFieldVectors();
        List<Field> fields = root.getSchema().getFields();
        DictionaryProvider[] columnDictionaries = new DictionaryProvider[vectors.size()];
        for (int col = 0; col < vectors.size(); col++) {
            columnDictionaries[col] = forColumn(fields.get(col), dictionaries);
        }
        int rowCount = root.getRowCount();
        for (int row = 0; row < rowCount; row++) {
            for (int col = 0; col < vectors.size(); col++) {
                if (col > 0) {
                    writer.write(TAB);
                }
                String value = format(vectors.get(col), row, columnDictionaries[col]);
                if (value != null) {
                    writer.write(value);
                }
            }
            writer.write(NEWLINE);
        }
    }

    /**
     * Formats a single cell as a string. Dates, times and timestamps are ISO-8601, always with
     * seconds ({@code 12:00:00}, {@code 2024-01-01T00:00:00}, {@code 2024-01-01T00:00:00Z}) and only
     * as many fraction digits as needed; all other types fall back to {@code getObject().toString()}
     * (readable for numerics, strings, booleans, lists, structs, maps). Null returns {@code null}.
     */
    public static String formatValue(FieldVector vector, int row) {
        return formatValue(vector, row, null);
    }

    /**
     * As {@link #formatValue(FieldVector, int)}, formatting a dictionary-encoded value as the
     * dictionary entry it refers to, looked up in {@code dictionaries}. A list, struct or map with a
     * dictionary-encoded value inside prints as {@code getObject().toString()} would with the
     * entries in place of the indices.
     */
    public static String formatValue(FieldVector vector, int row, DictionaryProvider dictionaries) {
        return format(vector, row, forColumn(vector.getField(), dictionaries));
    }

    /** {@link #formatValue}, with {@code dictionaries} already decided for the column by {@link #forColumn}. */
    private static String format(FieldVector vector, int row, DictionaryProvider dictionaries) {
        if (vector.isNull(row)) {
            return null;
        }
        Dictionary dictionary = dictionaries == NO_DICTIONARIES ? null : dictionaryOf(vector, dictionaries);
        if (dictionary != null) {
            return format(dictionary.getVector(), dictionaryIndex(vector, row), dictionaries);
        }
        return switch (vector.getMinorType()) {
            case DATEDAY ->
                    LocalDate.ofEpochDay(((DateDayVector) vector).get(row)).toString();
            case DATEMILLI ->
                    LocalDate.ofEpochDay(((DateMilliVector) vector).get(row) / 86_400_000L).toString();
            // ISO_LOCAL_TIME / ISO_LOCAL_DATE_TIME always print seconds; toString() drops them when
            // zero ("12:00", "2024-01-01T00:00"), which breaks clients parsing with a fixed pattern.
            case TIMESEC ->
                    ISO_LOCAL_TIME.format(LocalTime.ofSecondOfDay(((TimeSecVector) vector).get(row)));
            case TIMEMILLI ->
                    ISO_LOCAL_TIME.format(LocalTime.ofNanoOfDay((long) ((TimeMilliVector) vector).get(row) * 1_000_000L));
            case TIMEMICRO ->
                    ISO_LOCAL_TIME.format(LocalTime.ofNanoOfDay(((TimeMicroVector) vector).get(row) * 1_000L));
            case TIMENANO ->
                    ISO_LOCAL_TIME.format(LocalTime.ofNanoOfDay(((TimeNanoVector) vector).get(row)));
            // Arrow materializes non-TZ timestamps as LocalDateTime.
            case TIMESTAMPSEC, TIMESTAMPMILLI, TIMESTAMPMICRO, TIMESTAMPNANO ->
                    ISO_LOCAL_DATE_TIME.format((LocalDateTime) vector.getObject(row));
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
                Object value = dictionaries != NO_DICTIONARIES && hasDictionaryInside(vector.getField())
                        ? decodedObject(vector, row, dictionaries)
                        : vector.getObject(row);
                yield value != null ? value.toString() : null;
            }
        };
    }

    /**
     * The dictionary that {@code vector}'s values index into, or null when it is not
     * dictionary-encoded.
     *
     * @throws IllegalStateException when it is encoded but {@code dictionaries} lacks its dictionary
     */
    private static Dictionary dictionaryOf(ValueVector vector, DictionaryProvider dictionaries) {
        DictionaryEncoding encoding = vector.getField().getDictionary();
        if (encoding == null) {
            return null;
        }
        Dictionary dictionary = dictionaries == null ? null : dictionaries.lookup(encoding.getId());
        if (dictionary == null) {
            throw new IllegalStateException("Column '" + vector.getName() + "' is dictionary-encoded (id "
                    + encoding.getId() + ") but its dictionary was not provided");
        }
        return dictionary;
    }

    private static int dictionaryIndex(ValueVector indices, int row) {
        return Math.toIntExact(((BaseIntVector) indices).getValueAsLong(row));
    }

    /** Whether any column of {@code schema} is, or has inside it, a dictionary-encoded field. */
    public static boolean hasDictionary(Schema schema) {
        for (Field field : schema.getFields()) {
            if (hasDictionary(field)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasDictionary(Field field) {
        return field.getDictionary() != null || hasDictionaryInside(field);
    }

    /** Whether a dictionary-encoded field is nested anywhere inside {@code field}. */
    private static boolean hasDictionaryInside(Field field) {
        for (Field child : field.getChildren()) {
            if (hasDictionary(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The dictionaries to format the column {@code field} with: {@link #NO_DICTIONARIES} when it has
     * no dictionary-encoded value anywhere, so formatting skips every lookup; otherwise
     * {@code dictionaries}, after checking each such value can be resolved.
     *
     * @throws IllegalStateException when a dictionary-encoded value sits inside a type other than a
     *                               list, map, fixed-size list or struct
     */
    private static DictionaryProvider forColumn(Field field, DictionaryProvider dictionaries) {
        if (!hasDictionary(field)) {
            return NO_DICTIONARIES;
        }
        checkResolvable(field, field.getName());
        return dictionaries;
    }

    private static void checkResolvable(Field field, String column) {
        if (!hasDictionaryInside(field)) {
            return;
        }
        ArrowType.ArrowTypeID type = field.getType().getTypeID();
        // List views (DuckDB's arrow_output_list_view) are left out: they would need their own
        // offset-and-size walk, in TSV and JSON alike, and DuckDB does not produce them by default.
        if (type != ArrowType.ArrowTypeID.List && type != ArrowType.ArrowTypeID.LargeList
                && type != ArrowType.ArrowTypeID.Map && type != ArrowType.ArrowTypeID.FixedSizeList
                && type != ArrowType.ArrowTypeID.Struct) {
            throw new IllegalStateException("Column '" + column + "' has a dictionary-encoded value inside "
                    + type + ", which is not supported");
        }
        for (Field child : field.getChildren()) {
            checkResolvable(child, column);
        }
    }

    /**
     * {@code vector.getObject(index)}, but with each dictionary-encoded value replaced by its
     * dictionary entry, so the {@code toString()} TSV prints is unchanged apart from that. Mirrors
     * Arrow's {@code getObject} for lists, maps (a list of key/value structs) and structs (whose
     * null fields are left out).
     */
    private static Object decodedObject(ValueVector vector, int index, DictionaryProvider dictionaries) {
        if (vector.isNull(index)) {
            return null;
        }
        Dictionary dictionary = dictionaryOf(vector, dictionaries); // only reached for such a column
        if (dictionary != null) {
            return decodedObject(dictionary.getVector(), dictionaryIndex(vector, index), dictionaries);
        }
        if (!hasDictionaryInside(vector.getField())) {
            return vector.getObject(index);
        }
        switch (vector.getMinorType()) {
            case LIST, MAP -> {
                ListVector list = (ListVector) vector;
                JsonStringArrayList<Object> values = new JsonStringArrayList<>();
                for (int i = list.getElementStartIndex(index); i < list.getElementEndIndex(index); i++) {
                    values.add(decodedObject(list.getDataVector(), i, dictionaries));
                }
                return values;
            }
            case LARGELIST -> {
                LargeListVector list = (LargeListVector) vector;
                JsonStringArrayList<Object> values = new JsonStringArrayList<>();
                for (long i = list.getElementStartIndex(index); i < list.getElementEndIndex(index); i++) {
                    values.add(decodedObject(list.getDataVector(), Math.toIntExact(i), dictionaries));
                }
                return values;
            }
            case FIXED_SIZE_LIST -> {
                FixedSizeListVector list = (FixedSizeListVector) vector;
                int size = list.getListSize();
                JsonStringArrayList<Object> values = new JsonStringArrayList<>(size);
                for (int i = index * size; i < (index + 1) * size; i++) {
                    values.add(decodedObject(list.getDataVector(), i, dictionaries));
                }
                return values;
            }
            case STRUCT -> {
                JsonStringHashMap<String, Object> values = new JsonStringHashMap<>();
                for (FieldVector child : ((StructVector) vector).getChildrenFromFields()) {
                    Object value = decodedObject(child, index, dictionaries);
                    if (value != null) {
                        values.put(child.getName(), value);
                    }
                }
                return values;
            }
            // forColumn rejects a dictionary under any other type before a row is written.
            default -> throw new IllegalStateException("Column '" + vector.getName() + "' of type "
                    + vector.getMinorType() + " has a dictionary-encoded value inside, which is not supported");
        }
    }
}
