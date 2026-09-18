package io.dazzleduck.sql.commons.ingestion;

import java.util.Arrays;

/**
 * A fixed-size ring of per-wall-clock-minute counters, used to expose a short rolling history
 * (e.g. rows written per minute, batches received per minute) for the stats dashboards.
 *
 * <p>{@link #add} accumulates into the slot for the given instant's epoch-minute; {@link #snapshot}
 * returns the last {@code out} minutes ending at {@code nowMs}, oldest first, zero-filling any minute
 * with no activity (and any minute now older than the ring's window). Slots older than the window are
 * detected by the epoch-minute stamped on each slot, so a stale value is never mistaken for a recent
 * one. All methods are synchronized: increments come from a single writer in practice (the queue's
 * write thread, or {@code add()} under the queue lock), while the dashboard reads from another thread.
 */
final class MinuteRing {

    private static final long MS_PER_MINUTE = 60_000L;

    private final int window;
    private final long[] counts;
    private final long[] slotMinute; // epoch-minute currently held by each slot

    MinuteRing(int window) {
        if (window < 1) {
            throw new IllegalArgumentException("window must be >= 1");
        }
        this.window = window;
        this.counts = new long[window];
        this.slotMinute = new long[window];
        Arrays.fill(slotMinute, Long.MIN_VALUE);
    }

    synchronized void add(long epochMs, long amount) {
        long minute = Math.floorDiv(epochMs, MS_PER_MINUTE);
        int idx = (int) Math.floorMod(minute, window);
        if (slotMinute[idx] != minute) {
            slotMinute[idx] = minute;   // slot rolled over to a new minute — reset before accumulating
            counts[idx] = 0L;
        }
        counts[idx] += amount;
    }

    /**
     * Last {@code out} minutes ending at {@code nowMs}, oldest first. {@code out} must be
     * {@code <= window} so each returned minute maps to a distinct slot.
     */
    synchronized long[] snapshot(long nowMs, int out) {
        if (out > window) {
            throw new IllegalArgumentException("out (" + out + ") must be <= window (" + window + ")");
        }
        long nowMinute = Math.floorDiv(nowMs, MS_PER_MINUTE);
        long[] result = new long[out];
        for (int i = 0; i < out; i++) {
            long minute = nowMinute - (out - 1 - i);
            int idx = (int) Math.floorMod(minute, window);
            result[i] = slotMinute[idx] == minute ? counts[idx] : 0L;
        }
        return result;
    }
}
