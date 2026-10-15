package io.dazzleduck.sql.otel.collector;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The string copy writes numbers and bytes straight into {@link ArrowColumns.Bytes} instead of
 * going through a String, so its text must stay byte-for-byte what the String route gave:
 * {@link Long#toString}, {@link Double#toString} and lowercase {@link HexFormat}. Edge values
 * first, then a random sweep.
 */
class ArrowColumnsTest {

    private static String written(java.util.function.Consumer<ArrowColumns.Bytes> write) {
        ArrowColumns.Bytes bytes = new ArrowColumns.Bytes(1);   // tiny, so every write grows it
        write.accept(bytes);
        try (BufferAllocator allocator = new RootAllocator();
             ArrowBuf buffer = bytes.buffer(allocator)) {
            byte[] out = new byte[bytes.size()];
            buffer.getBytes(0, out);
            return new String(out, StandardCharsets.UTF_8);
        }
    }

    @Test
    void decimalMatchesLongToString() {
        long[] edges = {0, 1, -1, 9, 10, -9, -10, 99, 100, 1_000_000_007L, -1_000_000_007L,
                Integer.MAX_VALUE, Integer.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE + 1};
        for (long v : edges) {
            assertEquals(Long.toString(v), written(b -> b.addDecimal(v)), "edge " + v);
        }
        Random rng = new Random(7);
        for (int i = 0; i < 100_000; i++) {
            long v = rng.nextLong() >> rng.nextInt(64);   // every magnitude, both signs
            assertEquals(Long.toString(v), written(b -> b.addDecimal(v)));
        }
    }

    @Test
    void asciiMatchesDoubleToString() {
        double[] edges = {0.0, -0.0, 1.0, -1.5, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                Double.MIN_VALUE, Double.MAX_VALUE, 1e-300, 1e21, 1e7, 1e-3, 123456789.123};
        for (double v : edges) {
            assertEquals(Double.toString(v), written(b -> b.addAscii(Double.toString(v))), "edge " + v);
        }
    }

    @Test
    void hexMatchesHexFormat() {
        Random rng = new Random(11);
        for (int length = 0; length < 64; length++) {
            byte[] v = new byte[length];
            rng.nextBytes(v);
            assertEquals(HexFormat.of().formatHex(v), written(b -> b.addHex(v)), "length " + length);
        }
        byte[] all = new byte[256];
        for (int i = 0; i < 256; i++) all[i] = (byte) i;
        assertEquals(HexFormat.of().formatHex(all), written(b -> b.addHex(all)));
    }

    @Test
    void writesAppendInOrder() {
        assertEquals("ab-42ff1.5", written(b -> {
            b.add("ab".getBytes(StandardCharsets.UTF_8));
            b.addDecimal(-42);
            b.addHex(new byte[]{(byte) 0xff});
            b.addAscii("1.5");
        }));
    }

    /**
     * A vector's buffers are allocated inside {@link ArrowColumns#load}, so when one allocation fails
     * the ones already made are released: the allocator ends with nothing outstanding, where it
     * would otherwise report a leak when it is closed.
     */
    @Test
    void failedAllocationReleasesTheBuffersAlreadyMade() {
        try (BufferAllocator root = new RootAllocator();
             BufferAllocator small = root.newChildAllocator("small", 0, 1024);
             VarCharVector vector = new VarCharVector("v", small)) {
            assertThrows(OutOfMemoryException.class, () -> ArrowColumns.load(vector, 1, 0,
                    a -> a.buffer(64), a -> a.buffer(64), a -> a.buffer(4096)));
            assertEquals(0, small.getAllocatedMemory());
        }
    }
}
