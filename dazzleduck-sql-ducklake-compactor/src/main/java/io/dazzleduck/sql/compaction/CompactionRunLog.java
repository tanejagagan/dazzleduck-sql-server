package io.dazzleduck.sql.compaction;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory ring buffer of the last {@code capacity} {@link CompactionRun}s per {@code (database,
 * tier)} — control state, deliberately readable without touching the catalog or disk (per
 * COMPACTION_TELEMETRY_SPEC.md "Storage and Exposure"). The JSONL sink named in the spec is a
 * deferred follow-up; this is the always-on part.
 *
 * <p>{@link #aggregates} computes the spec's "Derived Quantities" over the retained window on demand,
 * for {@code /health} and the telemetry UI. Nothing here is persisted.
 */
public final class CompactionRunLog {

    /** {@code N = 50} default from the spec. */
    public static final int DEFAULT_CAPACITY = 50;

    public record Key(String database, String tierName) {}

    private final int capacity;
    private final Map<Key, Deque<CompactionRun>> byKey = new ConcurrentHashMap<>();

    public CompactionRunLog(int capacity) {
        this.capacity = Math.max(1, capacity);
    }

    public void record(CompactionRun run) {
        Deque<CompactionRun> dq = byKey.computeIfAbsent(new Key(run.database(), run.tierName()), k -> new ArrayDeque<>());
        synchronized (dq) {
            dq.addLast(run);
            while (dq.size() > capacity) {
                dq.removeFirst();
            }
        }
    }

    /** Keys that have at least one record, database then tier order. */
    public List<Key> keys() {
        return byKey.keySet().stream()
                .sorted(java.util.Comparator.comparing(Key::database).thenComparing(Key::tierName))
                .toList();
    }

    /** Retained runs for a key, oldest → newest (a copy; safe to iterate). */
    public List<CompactionRun> recent(Key key) {
        Deque<CompactionRun> dq = byKey.get(key);
        if (dq == null) {
            return List.of();
        }
        synchronized (dq) {
            return new ArrayList<>(dq);
        }
    }

    public CompactionRun latest(Key key) {
        Deque<CompactionRun> dq = byKey.get(key);
        if (dq == null) {
            return null;
        }
        synchronized (dq) {
            return dq.peekLast();
        }
    }

    /**
     * The spec's derived quantities over the retained window. Rates are computed over the runs that
     * carry the needed fields, so a window with some failed metadata reads still yields a figure from
     * the runs that succeeded. {@code durationHeadroom}'s denominator is the external commit timeout
     * carried on the latest run ({@code idle_in_transaction_session_timeout}), so it always reflects
     * the value the cycle actually raced — no separate config to drift.
     */
    public DerivedAggregates aggregates(Key key) {
        List<CompactionRun> runs = recent(key);
        if (runs.isEmpty()) {
            return DerivedAggregates.EMPTY;
        }
        long timeoutLimitMs = runs.get(runs.size() - 1).commitTimeoutMs();

        long filesRetired = 0, durationMsForFiles = 0, bytesRetired = 0, durationMsForBytes = 0;
        long empties = 0;
        Map<CompactionRun.FailureClass, Long> failures = new EnumMap<>(CompactionRun.FailureClass.class);
        List<Long> durations = new ArrayList<>(runs.size());
        for (CompactionRun r : runs) {
            durations.add(r.durationTotalMs());
            if (r.outcome() == CompactionRun.Outcome.EMPTY) {
                empties++;
            }
            if (r.outcome() == CompactionRun.Outcome.FAILED) {
                failures.merge(r.failureClass(), 1L, Long::sum);
            }
            if (r.filesRetired() != null && r.durationTotalMs() > 0) {
                filesRetired += r.filesRetired();
                durationMsForFiles += r.durationTotalMs();
            }
            Long br = r.bytesRetired();
            if (br != null && r.durationTotalMs() > 0) {
                bytesRetired += br;
                durationMsForBytes += r.durationTotalMs();
            }
        }

        double throughputFiles = durationMsForFiles > 0 ? filesRetired * 1000.0 / durationMsForFiles : 0;
        double throughputBytes = durationMsForBytes > 0 ? bytesRetired * 1000.0 / durationMsForBytes : 0;

        // Arrivals: files that appeared in the tier's file-size range between one cycle's end and the next cycle's start
        // (gap between run[n].activeFilesAfter and run[n+1].activeFilesBefore). No extra capture.
        long arrivals = 0, arrivalGapMs = 0;
        for (int i = 0; i + 1 < runs.size(); i++) {
            CompactionRun a = runs.get(i), b = runs.get(i + 1);
            if (a.activeFilesAfter() != null && b.activeFilesBefore() != null) {
                long delta = b.activeFilesBefore() - a.activeFilesAfter();
                if (delta > 0) {
                    arrivals += delta;
                }
                long gap = java.time.Duration.between(a.endedAt(), b.startedAt()).toMillis();
                if (gap > 0) {
                    arrivalGapMs += gap;
                }
            }
        }
        double arrivalFiles = arrivalGapMs > 0 ? arrivals * 1000.0 / arrivalGapMs : 0;

        durations.sort(Long::compareTo);
        int p95Index = Math.min(durations.size() - 1, Math.max(0, (int) Math.ceil(0.95 * durations.size()) - 1));
        long p95 = durations.get(p95Index);
        double headroom = timeoutLimitMs > 0 ? (double) p95 / timeoutLimitMs : 0;

        // Saturated: the scheduler is running cycles back-to-back, so cadence is no longer a control
        // variable. Judge on the most recent run's gap (fixed-rate collapses to ~0 once a cycle
        // outruns its interval).
        boolean saturated = runs.get(runs.size() - 1).actualGapMs() <= 1000;

        double idleRatio = (double) empties / runs.size();

        return new DerivedAggregates(runs.size(), throughputFiles, throughputBytes, arrivalFiles,
                throughputFiles - arrivalFiles, p95, headroom, saturated, idleRatio, failures);
    }

    /** Consumer-computed quantities over a window of runs — not stored on the record. */
    public record DerivedAggregates(
            int windowSize,
            double throughputFilesPerSec,
            double throughputBytesPerSec,
            double arrivalFilesPerSec,
            double drainFilesPerSec,
            long p95DurationMs,
            double durationHeadroom,
            boolean saturated,
            double idleRatio,
            Map<CompactionRun.FailureClass, Long> failuresByClass) {

        static final DerivedAggregates EMPTY =
                new DerivedAggregates(0, 0, 0, 0, 0, 0, 0, false, 0, Map.of());
    }
}
