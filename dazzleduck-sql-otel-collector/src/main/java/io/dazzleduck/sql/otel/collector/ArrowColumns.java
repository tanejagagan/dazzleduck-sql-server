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
        private byte[] data = new byte[4 * 256];   // entry 0 is 0, as a new array is zeroed
        private int count;

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
        private byte[] data = new byte[16 * 1024];
        private int size;

        void add(byte[] value) {
            int length = value.length;
            data = ensure(data, size + length);
            System.arraycopy(value, 0, data, size, length);
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
            BufferAllocator allocator = target.getAllocator();
            ArrowColumns.load(target, validity.count(), validity.nullCount(),
                    validity.validity(allocator), offsets.buffer(allocator), bytes.buffer(allocator));
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
            BufferAllocator allocator = target.getAllocator();
            ArrowColumns.load(target, validity.count(), validity.nullCount(),
                    validity.validity(allocator), values.buffer(allocator));
        }
    }

    /**
     * Loads {@code buffers} — validity first, in the vector's own buffer order — as its
     * {@code length} values. The vector takes its own reference to each buffer, so ours are
     * released either way.
     */
    static void load(FieldVector target, int length, int nullCount, ArrowBuf... buffers) {
        try {
            target.loadFieldBuffers(new ArrowFieldNode(length, nullCount), Arrays.asList(buffers));
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
