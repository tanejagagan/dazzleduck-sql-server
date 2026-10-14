package io.dazzleduck.sql.otel.collector;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Growable heap buffers already in Arrow's in-memory layout, so a whole batch can be accumulated
 * with plain array writes and handed to a vector in one copy per buffer through
 * {@link FieldVector#loadFieldBuffers} — the entry point the IPC reader uses. See
 * {@link AttributeMapsWriter} for why the collector writes this way instead of through the vectors.
 *
 * <p>Offsets are little-endian int32 starting at 0, values little-endian, bitmaps LSB-first —
 * Arrow's layout. None of these classes is thread-safe.
 */
final class ArrowColumns {

    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    /** Starting size of a buffer that holds a small part of a batch. */
    static final int DEFAULT_BYTES = 16 * 1024;
    static final int DEFAULT_ENTRIES = 256;
    /** Cap on {@link #initialBytes}: clients send batches of about 1 MB, most of it attribute text. */
    static final int MAX_INITIAL_BYTES = 512 * 1024;
    /** Attribute text of a typical record (~40 keys and values of ~12 bytes each), for sizing. */
    static final int BYTES_PER_ROW = 1024;
    /** Bytes per key or value, to size offsets from bytes. */
    static final int BYTES_PER_ENTRY = 16;

    /**
     * Starting size for the bulk of a batch — the keys and values of a top-level string attribute
     * map — from its row count. Growing them from {@link #DEFAULT_BYTES} by doubling on every batch
     * was ~13% of collector Java CPU; starting near the final size avoids those copies. A fixed
     * {@link #MAX_INITIAL_BYTES} start would instead allocate and zero ~2.5 MB per batch even for a
     * one-record export, so the start follows the row count, between {@link #DEFAULT_BYTES} and
     * {@link #MAX_INITIAL_BYTES}.
     */
    static int initialBytes(int expectedRows) {
        long bytes = (long) expectedRows * BYTES_PER_ROW;
        return (int) Math.max(DEFAULT_BYTES, Math.min(MAX_INITIAL_BYTES, bytes));
    }

    private static final byte[] HEX_DIGITS = "0123456789abcdef".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    private ArrowColumns() {
    }

    /** A bitmap: a validity buffer (bit set = present), or the values of a boolean column. */
    static final class Bits {
        private byte[] bits = new byte[64];
        private int count;
        private int clear;

        void add(boolean set) {
            bits = ensure(bits, (count >> 3) + 1);
            if (set) {
                bits[count >> 3] |= (byte) (1 << (count & 7));
            } else {
                clear++;
            }
            count++;
        }

        int count() {
            return count;
        }

        /** As a validity buffer: its null count. */
        int nullCount() {
            return clear;
        }

        /**
         * As a validity buffer. Empty when nothing is null, which {@code loadFieldBuffers} turns
         * into an all-valid bitmap of its own (see {@code BitVectorHelper.loadValidityBuffer}).
         */
        ArrowBuf validity(BufferAllocator allocator) {
            return clear == 0 ? allocator.getEmpty() : values(allocator);
        }

        /** The bitmap itself, as the values of a boolean column. */
        ArrowBuf values(BufferAllocator allocator) {
            return buffer(allocator, bits, (count + 7) >> 3);
        }
    }

    /** Offsets for a variable-width or list column: entry {@code i + 1} is where value {@code i} ends. */
    static final class Offsets {
        private byte[] data;   // entry 0 is 0, as a new array is zeroed
        private int count;

        Offsets() {
            this(DEFAULT_ENTRIES);
        }

        Offsets(int initialEntries) {
            data = new byte[4 * (initialEntries + 1)];
        }

        void add(int end) {
            count++;
            data = ensure(data, 4 * (count + 1));
            INT.set(data, 4 * count, end);
        }

        ArrowBuf buffer(BufferAllocator allocator) {
            return ArrowColumns.buffer(allocator, data, 4 * (count + 1));
        }
    }

    /** Concatenated variable-width values. */
    static final class Bytes {
        private byte[] data;
        private int size;

        Bytes() {
            this(DEFAULT_BYTES);
        }

        Bytes(int initialBytes) {
            data = new byte[initialBytes];
        }

        void add(byte[] value) {
            int length = value.length;
            data = ensure(data, size + length);
            System.arraycopy(value, 0, data, size, length);
            size += length;
        }

        /**
         * Appends {@code value} in decimal, byte for byte what {@link Long#toString(long)} gives,
         * without the intermediate String and its UTF-8 encoding.
         */
        void addDecimal(long value) {
            if (value == Long.MIN_VALUE) {   // its magnitude does not fit in a long
                addAscii("-9223372036854775808");
                return;
            }
            boolean negative = value < 0;
            long magnitude = negative ? -value : value;
            int digits = 1;
            for (long rest = magnitude / 10; rest != 0; rest /= 10) digits++;
            int length = digits + (negative ? 1 : 0);
            data = ensure(data, size + length);
            int at = size + length;
            do {
                data[--at] = (byte) ('0' + (int) (magnitude % 10));
                magnitude /= 10;
            } while (magnitude != 0);
            if (negative) data[--at] = '-';
            size += length;
        }

        /** Appends {@code text}, which must be ASCII — e.g. a {@link Double#toString(double)}. */
        void addAscii(String text) {
            int length = text.length();
            data = ensure(data, size + length);
            for (int i = 0; i < length; i++) {
                data[size + i] = (byte) text.charAt(i);
            }
            size += length;
        }

        /** Appends {@code value} as lowercase hex, as {@code HexFormat.of().formatHex} gives. */
        void addHex(byte[] value) {
            int length = value.length * 2;
            data = ensure(data, size + length);
            int at = size;
            for (byte b : value) {
                data[at++] = HEX_DIGITS[(b >> 4) & 0xF];
                data[at++] = HEX_DIGITS[b & 0xF];
            }
            size += length;
        }

        int size() {
            return size;
        }

        ArrowBuf buffer(BufferAllocator allocator) {
            return ArrowColumns.buffer(allocator, data, size);
        }
    }

    /** 8-byte values: BIGINT, or DOUBLE as {@link Double#doubleToRawLongBits}. */
    static final class Longs {
        private byte[] data = new byte[8 * 256];
        private int count;

        void add(long value) {
            data = ensure(data, 8 * (count + 1));
            LONG.set(data, 8 * count, value);
            count++;
        }

        ArrowBuf buffer(BufferAllocator allocator) {
            return ArrowColumns.buffer(allocator, data, 8 * count);
        }
    }

    /** A nullable VARCHAR / VARBINARY column. */
    static final class VarWidth {
        private final Bits validity = new Bits();
        private final Offsets offsets = new Offsets();
        private final Bytes bytes = new Bytes();

        /** Adds a value; {@code null} adds a null. */
        void add(byte[] value) {
            validity.add(value != null);
            if (value != null) bytes.add(value);
            offsets.add(bytes.size());
        }

        void load(FieldVector target) {
            ArrowColumns.load(target, validity.count(), validity.nullCount(),
                    validity::validity, offsets::buffer, bytes::buffer);
        }
    }

    /** A nullable BIGINT column. */
    static final class BigInts {
        private final Bits validity = new Bits();
        private final Longs values = new Longs();

        void add(long value) {
            validity.add(true);
            values.add(value);
        }

        void addNull() {
            validity.add(false);
            values.add(0);
        }

        void load(FieldVector target) {
            ArrowColumns.load(target, validity.count(), validity.nullCount(),
                    validity::validity, values::buffer);
        }
    }

    /** Allocates one of a vector's buffers, filled; see {@link #load}. */
    @FunctionalInterface
    interface BufferSource {
        ArrowBuf allocate(BufferAllocator allocator);
    }

    /**
     * Loads the buffers {@code sources} allocate — validity first, in the vector's own buffer order —
     * as the vector's {@code length} values. The buffers are allocated here, inside the
     * {@code try}, so that if one allocation fails the ones already made are still released. The
     * vector takes its own reference to each buffer, so ours are released either way.
     */
    static void load(FieldVector target, int length, int nullCount, BufferSource... sources) {
        BufferAllocator allocator = target.getAllocator();
        ArrowBuf[] buffers = new ArrowBuf[sources.length];
        try {
            for (int i = 0; i < sources.length; i++) {
                buffers[i] = sources[i].allocate(allocator);
            }
            target.loadFieldBuffers(new ArrowFieldNode(length, nullCount), Arrays.asList(buffers));
        } finally {
            for (ArrowBuf buffer : buffers) {
                if (buffer != null) buffer.close();
            }
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
