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

    private static final CompactionTier MINOR = new CompactionTier(
            "minor", true, Duration.ofSeconds(60), 0, 512 * 1024L, 0, List.of());
    private static final CompactionTier MAJOR = new CompactionTier(
            "major", true, Duration.ofSeconds(60), 512 * 1024L, 10 * 1024 * 1024L, 0, List.of());

    private static final CompactionConfig CONFIG = new CompactionConfig(
            List.of(DB),
            List.of(MINOR, MAJOR),
            Duration.ofSeconds(60),
            Duration.ofSeconds(5),
            List.of(),
            0,
            Duration.ofSeconds(30),
            CompactionRunLog.DEFAULT_CAPACITY);

    // JUnit builds a fresh test instance per method, so these are per-test state.
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final CompactionState state = new CompactionState(registry, List.of(DB), List.of("minor", "major"));
    private final CompactionRunLog runLog = new CompactionRunLog(10);

    /**
     * Never touches the database. The surrounding cycle still queries DuckLake metadata for the
     * file counts, but that catalog is not attached here and those queries swallow their own
     * errors, so the logged catalog failures during this test are expected.
     */
    private static TierCompactor compactor(boolean fail) {
        return (database, tier) -> {
            if (fail) throw new IllegalStateException("tier '" + tier.name() + "' compaction blew up");
            return new TierCompactor.MergeOutcome(1, -1, null);
        };
    }

    private static Housekeeper housekeeper(boolean fail) {
        return database -> {
            if (fail) throw new IllegalStateException("housekeeping blew up");
        };
    }

    private double failures(String kind) {
        return registry.get("ducklake.compaction.failures")
                .tag("database", DB).tag("type", kind).functionCounter().count();
    }

    private double successes(String tierName) {
        return registry.get("ducklake.compaction.cycles")
                .tag("database", DB).tag("tier", tierName).functionCounter().count();
    }

    @Test
    void failedTierCycleCountsAsFailureAndLeavesLastSuccessUnset() {
        try (CompactionService service = new CompactionService(CONFIG, null, compactor(true), housekeeper(false), state, runLog)) {
            service.runTier(DB, MAJOR);

            assertEquals(1, failures("major"), "the failed cycle should be counted");
            assertEquals(0, successes("major"), "a cycle that threw must not count as a successful compaction");
            assertEquals(0, successes("minor"));
            assertNull(state.getLastSuccessTime(DB),
                    "an alert on last success must not be satisfied by a failed cycle");
            assertEquals(1, state.getFailureCount(DB));
        }
    }

    @Test
    void differentTiersFailIndependently() {
        try (CompactionService service = new CompactionService(CONFIG, null, compactor(true), housekeeper(false), state, runLog)) {
            service.runTier(DB, MINOR);

            assertEquals(1, failures("minor"), "the failed cycle should be counted");
            assertEquals(0, failures("major"), "a different tier's failure must not be blamed on this one");
            assertNull(state.getLastSuccessTime(DB));
            assertEquals(1, state.getFailureCount(DB));
        }
    }

    @Test
    void failedHousekeepingIsAttributedToItsOwnKind() {
        try (CompactionService service = new CompactionService(CONFIG, null, compactor(false), housekeeper(true), state, runLog)) {
            service.runHousekeeping(DB);

            assertEquals(1, failures(CompactionState.HOUSEKEEPING_KIND));
            assertEquals(0, failures("major"), "housekeeping must not be blamed on a tier");
            assertNull(state.getLastSuccessTime(DB),
                    "housekeeping is not a compaction cycle and does not mark success");
        }
    }

    @Test
    void successfulCycleCountsAndStampsLastSuccess() {
        try (CompactionService service = new CompactionService(CONFIG, null, compactor(false), housekeeper(false), state, runLog)) {
            service.runTier(DB, MAJOR);

            assertEquals(1, successes("major"));
            assertEquals(0, state.getFailureCount(DB));
            assertNotNull(state.getLastSuccessTime(DB));
        }
    }

    @Test
    void nextExecutionTimeIsReportedAfterAFailedCycle() {
        // A database whose cycles fail is still scheduled to run again, so /health must report a
        // next-execution time rather than null just because there has been no success.
        try (CompactionService service = new CompactionService(CONFIG, null, compactor(true), housekeeper(false), state, runLog)) {
            service.runTier(DB, MAJOR);

            assertNull(state.getLastSuccessTime(DB), "the failed cycle leaves last success unset");
            assertNotNull(state.getLastRunTime(DB), "every cycle stamps its completion time");

            CompactionStats.DatabaseStats ds = service.getStats().databases().get(DB);
            assertNotNull(ds.nextExecutionTimeByTier().get("major"),
                    "a failing tier is still scheduled, so next execution must not be null");
        }
    }

    @Test
    void nextExecutionTimeIsPerTierAndOnlyPopulatedForTiersThatHaveRun() {
        try (CompactionService service = new CompactionService(CONFIG, null, compactor(true), housekeeper(false), state, runLog)) {
            service.runTier(DB, MAJOR);

            CompactionStats.DatabaseStats ds = service.getStats().databases().get(DB);
            Instant lastRun = state.getLastRunTime(DB);
            assertEquals(lastRun.plus(MAJOR.frequency()), ds.nextExecutionTimeByTier().get("major"));
            assertNull(ds.nextExecutionTimeByTier().get("minor"), "minor never ran, so it has no next-execution estimate yet");
        }
    }

    @Test
    void nextExecutionTimeIsEmptyWhenNoTierIsEnabled() {
        CompactionConfig neitherEnabledConfig = new CompactionConfig(
                CONFIG.databases(),
                List.of(new CompactionTier("minor", false, MINOR.frequency(), MINOR.minFileSize(), MINOR.maxFileSize(), 0, List.of()),
                        new CompactionTier("major", false, MAJOR.frequency(), MAJOR.minFileSize(), MAJOR.maxFileSize(), 0, List.of())),
                CONFIG.housekeepingFrequency(), CONFIG.snapshotRetention(), CONFIG.housekeepingConnectionSettings(),
                CONFIG.healthPort(), CONFIG.fileCountRefreshFrequency(), CONFIG.runHistorySize());

        try (CompactionService service = new CompactionService(neitherEnabledConfig, null, compactor(true), housekeeper(false), state, runLog)) {
            service.runHousekeeping(DB);

            CompactionStats.DatabaseStats ds = service.getStats().databases().get(DB);
            assertTrue(ds.nextExecutionTimeByTier().isEmpty(), "nothing is scheduled, so there's no next execution to report");
        }
    }

    @Test
    void cycleUpdatesOutcomeCounterAndControlInputGauges() {
        try (CompactionService service = new CompactionService(CONFIG, null, compactor(false), housekeeper(false), state, runLog)) {
            service.runTier(DB, MAJOR);

            assertEquals(1, registry.get("ducklake.compaction.cycles_by_outcome")
                    .tag("database", DB).tag("tier", "major").tag("outcome", "SUCCESS").functionCounter().count());
            // Control inputs are emitted now (they become dynamic later): frequency reflects the tier.
            assertEquals(MAJOR.frequency().toMillis(), registry.get("ducklake.compaction.tier_frequency")
                    .tag("database", DB).tag("tier", "major").gauge().value());
            assertEquals(MAJOR.maxCompactedFiles(), registry.get("ducklake.compaction.groups_requested")
                    .tag("database", DB).tag("tier", "major").gauge().value());
        }
    }

    @Test
    void failedCycleIsCountedByOutcomeToo() {
        try (CompactionService service = new CompactionService(CONFIG, null, compactor(true), housekeeper(false), state, runLog)) {
            service.runTier(DB, MAJOR);
            assertEquals(1, registry.get("ducklake.compaction.cycles_by_outcome")
                    .tag("database", DB).tag("tier", "major").tag("outcome", "FAILED").functionCounter().count());
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
