package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.dazzleduck.sql.otel.collector.MapColumnWriter.utf8;

/**
 * Writes one OTLP attribute set into its six map columns, routing each attribute by its
 * {@code AnyValue} kind (see {@link AttributeType}):
 * <ul>
 *   <li>{@code attributes} — string values, and with {@code stringCopy} every other value too,
 *       rendered as text exactly as before the typed maps existed;</li>
 *   <li>{@code attributes_int}, {@code _double}, {@code _bool}, {@code _bytes} — the native value;</li>
 *   <li>{@code attributes_json} — arrays and key/value lists as typed JSON.</li>
 * </ul>
 * Every row gets a map in all six columns, empty when it has no attribute of that kind. An
 * attribute with no value set has no typed column, so it is kept in the string map as an empty
 * string whatever {@code stringCopy} says.
 *
 * <p>WHY IT BUFFERS THE BATCH. The collector is CPU-bound in Java, and with ~30-60 attributes per
 * record this is its hottest loop. Writing each entry into the Arrow vectors costs several
 * bounds-checked buffer operations per entry ({@code setIndexDefined}, then {@code setSafe} on the
 * key and on the value, each checking capacity, filling offset holes and touching a validity
 * bitmap), plus a {@code startNewValue}/{@code endValue} pair per row of each map. With six maps
 * per attribute set that bookkeeping, not the data, was the cost: written entry by entry through
 * {@link MapColumnWriter}, the six maps made the whole batch writer 24-37% slower than the single
 * string map they replaced. So the batch is accumulated in plain heap arrays already in Arrow's
 * layout — offsets through a little-endian {@link VarHandle} view, bytes by
 * {@code System.arraycopy} — and {@link #finish} hands each Arrow buffer over in ONE copy through
 * {@link FieldVector#loadFieldBuffers}, the entry point the IPC reader uses. Nothing here is ever
 * null (entries, keys and values all exist), so every validity bitmap is all-valid, which
 * {@code loadFieldBuffers} builds itself from a null count of 0.
 *
 * <p>Usage: {@link #write} every row, in order from 0, then {@link #finish} once, BEFORE
 * {@code root.setRowCount}. NOT thread-safe; one instance per attribute set per batch.
 */
final class AttributeMapsWriter {

    private final boolean stringCopy;
    private final MapBuilder strings;
    private final MapBuilder ints;
    private final MapBuilder doubles;
    private final MapBuilder bools;
    private final MapBuilder bytes;
    private final MapBuilder json;
    private int rows;

    private AttributeMapsWriter(VectorSchemaRoot root, int stringColumn, int typedStart, boolean stringCopy) {
        this.stringCopy = stringCopy;
        this.strings = new MapBuilder(AttributeType.STRING, (MapVector) root.getVector(stringColumn));
        this.ints    = new MapBuilder(AttributeType.INT,    (MapVector) root.getVector(typedStart));
        this.doubles = new MapBuilder(AttributeType.DOUBLE, (MapVector) root.getVector(typedStart + 1));
        this.bools   = new MapBuilder(AttributeType.BOOL,   (MapVector) root.getVector(typedStart + 2));
        this.bytes   = new MapBuilder(AttributeType.BYTES,  (MapVector) root.getVector(typedStart + 3));
        this.json    = new MapBuilder(AttributeType.JSON,   (MapVector) root.getVector(typedStart + 4));
    }

    /**
     * @param stringColumn index of the string map
     * @param typedStart   index of the first of the five typed maps, in {@link AttributeType#TYPED} order
     * @param stringCopy   also write every non-string value, as text, to the string map
     */
    static AttributeMapsWriter of(VectorSchemaRoot root, int stringColumn, int typedStart, boolean stringCopy) {
        return new AttributeMapsWriter(root, stringColumn, typedStart, stringCopy);
    }

    /** Buffers row {@code row}'s attributes; rows must arrive in order from 0 with no gaps. */
    void write(int row, List<KeyValue> kvList) {
        if (row != rows) {
            throw new IllegalStateException("attribute rows must be written in order: expected " + rows + ", got " + row);
        }
        int count = kvList.size();
        // Indexed, not for-each: protobuf's list is RandomAccess (see MapColumnWriter).
        for (int i = 0; i < count; i++) {
            KeyValue kv = kvList.get(i);
            AnyValue value = kv.getValue();
            byte[] key = utf8(kv.getKey());
            switch (value.getValueCase()) {
                case STRING_VALUE -> strings.putVar(key, utf8(value.getStringValue()));
                case INT_VALUE -> {
                    ints.putLong(key, value.getIntValue());
                    copy(key, value);
                }
                case DOUBLE_VALUE -> {
                    doubles.putLong(key, Double.doubleToRawLongBits(value.getDoubleValue()));
                    copy(key, value);
                }
                case BOOL_VALUE -> {
                    bools.putBool(key, value.getBoolValue());
                    copy(key, value);
                }
                case BYTES_VALUE -> {
                    bytes.putVar(key, value.getBytesValue().toByteArray());
                    copy(key, value);
                }
                case ARRAY_VALUE, KVLIST_VALUE -> {
                    byte[] text = utf8(LogRecordConverter.anyValueToJson(value));
                    json.putVar(key, text);
                    if (stringCopy) strings.putVar(key, text);
                }
                default -> strings.putVar(key, utf8(LogRecordConverter.anyValueToString(value)));
            }
        }
        strings.endRow();
        ints.endRow();
        doubles.endRow();
        bools.endRow();
        bytes.endRow();
        json.endRow();
        rows++;
    }

    /**
     * Loads the buffered batch into the six map vectors. {@code rowCount} is the count about to be
     * passed to {@code root.setRowCount}; a mismatch would misalign the maps against every other
     * column, so it fails instead.
     */
    void finish(int rowCount) {
        if (rowCount != rows) {
            throw new IllegalStateException("wrote attributes for " + rows + " rows, batch has " + rowCount);
        }
        strings.load();
        ints.load();
        doubles.load();
        bools.load();
        bytes.load();
        json.load();
    }

    private void copy(byte[] key, AnyValue value) {
        if (stringCopy) strings.putVar(key, utf8(LogRecordConverter.anyValueToString(value)));
    }

    /**
     * One map column's batch in heap arrays, in Arrow's own layout so {@link #load} is a straight
     * copy: little-endian int32 offsets, concatenated key/value bytes, 8-byte little-endian values
     * for int and double, a packed bitmap for bool.
     */
    private static final class MapBuilder {

        private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
        private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

        private final AttributeType type;
        private final MapVector vector;

        private int rows;
        private int entries;
        private byte[] rowOffsets = new byte[4 * 256];   // offset 0 is 0, as a new array is zeroed
        private byte[] keyOffsets = new byte[4 * 1024];
        private byte[] keyData = new byte[16 * 1024];
        private int keyBytes;
        // String, bytes and json values: offsets + data. Int, double and bool: values.
        private byte[] valueOffsets;
        private byte[] valueData;
        private int valueBytes;
        private byte[] values;

        MapBuilder(AttributeType type, MapVector vector) {
            this.type = type;
            this.vector = vector;
            switch (type) {
                case INT, DOUBLE -> values = new byte[8 * 256];
                case BOOL -> values = new byte[32];
                default -> {
                    valueOffsets = new byte[4 * 1024];
                    valueData = new byte[16 * 1024];
                }
            }
        }

        void putVar(byte[] key, byte[] value) {
            putKey(key);
            int length = value.length;
            valueData = ensure(valueData, valueBytes + length);
            System.arraycopy(value, 0, valueData, valueBytes, length);
            valueBytes += length;
            valueOffsets = ensure(valueOffsets, 4 * (entries + 1));
            INT.set(valueOffsets, 4 * entries, valueBytes);
        }

        void putLong(byte[] key, long value) {
            putKey(key);
            values = ensure(values, 8 * entries);
            LONG.set(values, 8 * (entries - 1), value);
        }

        void putBool(byte[] key, boolean value) {
            putKey(key);
            int bit = entries - 1;
            values = ensure(values, (bit >> 3) + 1);
            if (value) values[bit >> 3] |= (byte) (1 << (bit & 7));
        }

        /** Appends the key and opens the entry; its value goes in slot {@code entries - 1}. */
        private void putKey(byte[] key) {
            int length = key.length;
            keyData = ensure(keyData, keyBytes + length);
            System.arraycopy(key, 0, keyData, keyBytes, length);
            keyBytes += length;
            entries++;
            keyOffsets = ensure(keyOffsets, 4 * (entries + 1));
            INT.set(keyOffsets, 4 * entries, keyBytes);
        }

        void endRow() {
            rows++;
            rowOffsets = ensure(rowOffsets, 4 * (rows + 1));
            INT.set(rowOffsets, 4 * rows, entries);
        }

        /** Hands every buffer to its vector: the map, its entry struct, the key and the value. */
        void load() {
            BufferAllocator allocator = vector.getAllocator();
            StructVector struct = (StructVector) vector.getDataVector();
            FieldVector keyVector = (FieldVector) struct.getChild(MapVector.KEY_NAME);
            FieldVector valueVector = (FieldVector) struct.getChild(MapVector.VALUE_NAME);

            load(vector, rows, buffer(allocator, rowOffsets, 4 * (rows + 1)));
            load(struct, entries);
            load(keyVector, entries,
                    buffer(allocator, keyOffsets, 4 * (entries + 1)), buffer(allocator, keyData, keyBytes));
            switch (type) {
                case INT, DOUBLE -> load(valueVector, entries, buffer(allocator, values, 8 * entries));
                case BOOL -> load(valueVector, entries, buffer(allocator, values, (entries + 7) >> 3));
                default -> load(valueVector, entries,
                        buffer(allocator, valueOffsets, 4 * (entries + 1)), buffer(allocator, valueData, valueBytes));
            }
        }

        /**
         * Loads {@code buffers} after an empty validity buffer, which with a null count of 0 makes
         * Arrow allocate an all-valid bitmap. The vector takes its own reference to each buffer,
         * so ours are released either way.
         */
        private static void load(FieldVector target, int length, ArrowBuf... buffers) {
            try {
                List<ArrowBuf> own = new ArrayList<>(buffers.length + 1);
                own.add(target.getAllocator().getEmpty());
                own.addAll(Arrays.asList(buffers));
                target.loadFieldBuffers(new ArrowFieldNode(length, 0), own);
            } finally {
                for (ArrowBuf buffer : buffers) buffer.close();
            }
        }

        private static ArrowBuf buffer(BufferAllocator allocator, byte[] source, int length) {
            ArrowBuf buffer = allocator.buffer(length);
            buffer.setBytes(0, source, 0, length);
            return buffer;
        }

        private static byte[] ensure(byte[] array, int needed) {
            return needed <= array.length ? array : Arrays.copyOf(array, Math.max(needed, array.length * 2));
        }
    }
}
