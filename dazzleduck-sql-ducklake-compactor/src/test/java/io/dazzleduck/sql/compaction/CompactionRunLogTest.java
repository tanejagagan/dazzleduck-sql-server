package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CompactionRunLogTest {

    private static final CompactionRunLog.Key KEY = new CompactionRunLog.Key("db", "tier");

    @Test
    void ringEvictsOldestBeyondCapacity() {
        CompactionRunLog log = new CompactionRunLog(3);
        for (int i = 1; i <= 5; i++) {
            log.record(TestRuns.run(i, i, i, 0, 100L, 90L, 0L, 0L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        }
        var recent = log.recent(KEY);
        assertEquals(3, recent.size(), "only the last 3 retained");
        assertEquals(3, recent.get(0).runId(), "oldest retained is run 3");
        assertEquals(5, log.latest(KEY).runId());
    }

    @Test
    void keysAreListedAndIsolatedPerDatabaseTier() {
        CompactionRunLog log = new CompactionRunLog(10);
        log.record(TestRuns.run(1, 0, 1, 0, 10L, 5L, 0L, 0L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        assertEquals(1, log.keys().size());
        assertEquals(KEY, log.keys().get(0));
    }

    @Test
    void derivedAggregatesMatchTheSpecFormulas() {
        CompactionRunLog log = new CompactionRunLog(50);
        // run1: [0s,2s] retired 80 files, 800 bytes
        log.record(TestRuns.run(1, 0, 2, 2000, 100L, 20L, 1000L, 200L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        // run2: [10s,13s] retired 40; 30 files arrived over the 8s gap since run1 ended
        log.record(TestRuns.run(2, 10, 13, 8000, 50L, 10L, 500L, 100L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        // run3: [20s,21s] retired 0 -> EMPTY; 15 files arrived over the 7s gap since run2 ended
        log.record(TestRuns.run(3, 20, 21, 7000, 25L, 25L, 250L, 250L, CompactionRun.Outcome.EMPTY, CompactionRun.FailureClass.NONE));

        var agg = log.aggregates(KEY);
        assertEquals(3, agg.windowSize());
        // throughput = (80+40+0) files / (2+3+1) s = 20 f/s
        assertEquals(20.0, agg.throughputFilesPerSec(), 0.001);
        // arrivals = (50-20)+(25-10)=45 over (8+7)=15s = 3 f/s
        assertEquals(3.0, agg.arrivalFilesPerSec(), 0.001);
        assertEquals(17.0, agg.drainFilesPerSec(), 0.001);
        assertEquals(1.0 / 3, agg.idleRatio(), 0.001);
        assertEquals(3000, agg.p95DurationMs());
        assertEquals(3000.0 / 120000, agg.durationHeadroom(), 0.0001);
        assertFalse(agg.saturated(), "latest gap 7000ms > 1s");
    }

    @Test
    void saturatedWhenLatestGapNearZero() {
        CompactionRunLog log = new CompactionRunLog(10);
        log.record(TestRuns.run(1, 0, 5, 0, 100L, 10L, 0L, 0L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        assertTrue(log.aggregates(KEY).saturated());
    }

    @Test
    void failuresAreCountedByClass() {
        CompactionRunLog log = new CompactionRunLog(10);
        log.record(TestRuns.run(1, 0, 1, 100, null, null, null, null, CompactionRun.Outcome.FAILED, CompactionRun.FailureClass.OUT_OF_MEMORY));
        log.record(TestRuns.run(2, 2, 3, 100, null, null, null, null, CompactionRun.Outcome.FAILED, CompactionRun.FailureClass.OUT_OF_MEMORY));
        log.record(TestRuns.run(3, 4, 5, 100, null, null, null, null, CompactionRun.Outcome.FAILED, CompactionRun.FailureClass.COMMIT_TIMEOUT));
        var byClass = log.aggregates(KEY).failuresByClass();
        assertEquals(2L, byClass.get(CompactionRun.FailureClass.OUT_OF_MEMORY));
        assertEquals(1L, byClass.get(CompactionRun.FailureClass.COMMIT_TIMEOUT));
    }

    @Test
    void emptyKeyYieldsEmptyAggregates() {
        CompactionRunLog log = new CompactionRunLog(10);
        assertEquals(0, log.aggregates(KEY).windowSize());
        assertTrue(log.recent(KEY).isEmpty());
        assertNull(log.latest(KEY));
    }
}
