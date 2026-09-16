package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers the distinction the compaction counters used to miss: a cycle that throws must not count
 * as a success, and must not refresh the last-success timestamp an alert watches.
 */
class CompactionOutcomeTest {

    private static final String DB = "outcome_test";

    private static final CompactionConfig CONFIG = new CompactionConfig(
            List.of(DB),
            Duration.ofSeconds(60),
            Duration.ofSeconds(60),
            Duration.ofSeconds(60),
            512 * 1024L,
            0,
            10 * 1024 * 1024L,
            0,
            Duration.ofSeconds(5),
            0,
            true,
            List.of(),
            true,
            List.of());

    // JUnit builds a fresh test instance per method, so these are per-test state.
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final CompactionState state = new CompactionState(registry, List.of(DB));

    /**
     * Neither method touches the database. The surrounding cycle still queries DuckLake metadata for
     * the file counts, but that catalog is not attached here and those queries swallow their own
     * errors, so the logged catalog failures during this test are expected.
     */
    private static MajorCompactor compactor(boolean fail) {
        return new MajorCompactor() {
            @Override
            public void compact(String database) throws Exception {
                if (fail) throw new IllegalStateException("major compaction blew up");
            }

            @Override
            public void housekeep(String database) throws Exception {
                if (fail) throw new IllegalStateException("housekeeping blew up");
            }
        };
    }

    private double failures(CycleKind kind) {
        return registry.get("ducklake.compaction.failures")
                .tag("database", DB).tag("type", kind.tag()).functionCounter().count();
    }

    private double successes(String metric) {
        return registry.get(metric).tag("database", DB).functionCounter().count();
    }

    @Test
    void failedMajorCycleCountsAsFailureAndLeavesLastSuccessUnset() {
        try (CompactionService service = new CompactionService(CONFIG, compactor(true), state)) {
            service.runMajor(DB);

            assertEquals(1, failures(CycleKind.MAJOR), "the failed cycle should be counted");
            assertEquals(0, successes("ducklake.compaction.major"),
                    "a cycle that threw must not count as a successful major compaction");
            assertEquals(0, successes("ducklake.compaction.minor"));
            assertNull(state.getLastSuccessTime(DB),
                    "an alert on last success must not be satisfied by a failed cycle");
            assertEquals(1, state.getFailureCount(DB));
        }
    }

    @Test
    void failedMinorCycleCountsAsFailureAndLeavesLastSuccessUnset() {
        // Unlike major, minor never goes through the injectable MajorCompactor — it builds its own
        // ducklake_merge_adjacent_files call directly. DB is never attached in this unit test class
        // (see the class javadoc), so this fails naturally without needing a fake to throw.
        try (CompactionService service = new CompactionService(CONFIG, compactor(false), state)) {
            service.runMinor(DB);

            assertEquals(1, failures(CycleKind.MINOR), "the failed cycle should be counted");
            assertEquals(0, successes("ducklake.compaction.minor"));
            assertNull(state.getLastSuccessTime(DB));
            assertEquals(1, state.getFailureCount(DB));
        }
    }

    @Test
    void failedHousekeepingIsAttributedToItsOwnCycleKind() {
        try (CompactionService service = new CompactionService(CONFIG, compactor(true), state)) {
            service.runHousekeeping(DB);

            assertEquals(1, failures(CycleKind.HOUSEKEEPING));
            assertEquals(0, failures(CycleKind.MAJOR), "housekeeping must not be blamed on major");
            assertNull(state.getLastSuccessTime(DB),
                    "housekeeping is not a compaction cycle and does not mark success");
        }
    }

    @Test
    void successfulCycleCountsAndStampsLastSuccess() {
        try (CompactionService service = new CompactionService(CONFIG, compactor(false), state)) {
            service.runMajor(DB);

            assertEquals(1, successes("ducklake.compaction.major"));
            assertEquals(0, state.getFailureCount(DB));
            assertNotNull(state.getLastSuccessTime(DB));
        }
    }

    @Test
    void nextExecutionTimeIsReportedAfterAFailedCycle() {
        // A database whose cycles fail is still scheduled to run again, so /health must report a
        // next-execution time rather than null just because there has been no success.
        try (CompactionService service = new CompactionService(CONFIG, compactor(true), state)) {
            service.runMajor(DB);

            assertNull(state.getLastSuccessTime(DB), "the failed cycle leaves last success unset");
            assertNotNull(state.getLastRunTime(DB), "every cycle stamps its completion time");

            CompactionStats.DatabaseStats ds = service.getStats().databases().get(DB);
            assertNotNull(ds.nextExecutionTime(),
                    "a failing database is still scheduled, so next execution must not be null");
        }
    }

    @Test
    void nextExecutionTimeFallsBackToMajorFrequencyWhenMinorIsDisabled() {
        CompactionConfig majorOnlyConfig = new CompactionConfig(
                CONFIG.databases(), CONFIG.minorCompactionFrequency(), Duration.ofSeconds(30),
                CONFIG.housekeepingFrequency(), CONFIG.minorCompactionMaxSize(), CONFIG.minorCompactionMaxFiles(),
                CONFIG.majorCompactionMaxSize(), CONFIG.majorCompactionMaxFiles(), CONFIG.snapshotRetention(),
                CONFIG.healthPort(), false, CONFIG.minorConnectionSettings(), true, CONFIG.majorConnectionSettings());

        try (CompactionService service = new CompactionService(majorOnlyConfig, compactor(true), state)) {
            service.runMajor(DB);

            CompactionStats.DatabaseStats ds = service.getStats().databases().get(DB);
            Instant lastRun = state.getLastRunTime(DB);
            assertEquals(lastRun.plus(Duration.ofSeconds(30)), ds.nextExecutionTime(),
                    "with minor disabled, next execution should be derived from major's own frequency");
        }
    }

    @Test
    void nextExecutionTimeIsNullWhenNeitherMinorNorMajorIsEnabled() {
        CompactionConfig neitherEnabledConfig = new CompactionConfig(
                CONFIG.databases(), CONFIG.minorCompactionFrequency(), CONFIG.majorCompactionFrequency(),
                CONFIG.housekeepingFrequency(), CONFIG.minorCompactionMaxSize(), CONFIG.minorCompactionMaxFiles(),
                CONFIG.majorCompactionMaxSize(), CONFIG.majorCompactionMaxFiles(), CONFIG.snapshotRetention(),
                CONFIG.healthPort(), false, CONFIG.minorConnectionSettings(), false, CONFIG.majorConnectionSettings());

        try (CompactionService service = new CompactionService(neitherEnabledConfig, compactor(true), state)) {
            service.runHousekeeping(DB);

            CompactionStats.DatabaseStats ds = service.getStats().databases().get(DB);
            assertNull(ds.nextExecutionTime(), "nothing is scheduled, so there's no next execution to report");
        }
    }

    @Test
    void lastSuccessAgeIsRegisteredBeforeAnySuccessHasHappened() {
        // Registered up front rather than on first success, so a compactor that has never
        // succeeded still reports an age instead of no series at all.
        assertTrue(registry.get("ducklake.compaction.last_success_age")
                .tag("database", DB).gauge().value() >= 0);
    }
}
