package io.dazzleduck.sql.commons.ingestion;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MinuteRingTest {

    private static final long MIN = 60_000L;

    @Test
    public void accumulatesWithinAMinuteAndZeroFillsIdle() {
        MinuteRing ring = new MinuteRing(15);
        long t0 = 100 * MIN; // some minute boundary
        ring.add(t0 + 1_000, 100);
        ring.add(t0 + 30_000, 50); // same minute -> 150
        ring.add(t0 + MIN + 5_000, 200); // next minute

        long[] snap = ring.snapshot(t0 + MIN + 10_000, 3); // minutes t0-1, t0, t0+1 (oldest first)
        assertArrayEquals(new long[]{0, 150, 200}, snap);
    }

    @Test
    public void snapshotReturnsRequestedWindowOldestFirst() {
        MinuteRing ring = new MinuteRing(15);
        long base = 1000 * MIN;
        for (int i = 0; i < 15; i++) {
            ring.add(base + i * MIN, (i + 1) * 10L); // minute i -> (i+1)*10
        }
        long[] snap = ring.snapshot(base + 14 * MIN, 15);
        for (int i = 0; i < 15; i++) {
            assertEquals((i + 1) * 10L, snap[i], "minute " + i);
        }
    }

    @Test
    public void staleSlotsBeyondWindowReadAsZero() {
        MinuteRing ring = new MinuteRing(15);
        long base = 500 * MIN;
        ring.add(base, 999); // minute 500
        // 20 minutes later, that slot's minute no longer matches -> zero, not the stale 999.
        long[] snap = ring.snapshot(base + 20 * MIN, 15);
        for (long v : snap) {
            assertEquals(0, v);
        }
    }

    @Test
    public void snapshotOutMustNotExceedWindow() {
        MinuteRing ring = new MinuteRing(5);
        assertThrows(IllegalArgumentException.class, () -> ring.snapshot(0, 6));
    }
}
