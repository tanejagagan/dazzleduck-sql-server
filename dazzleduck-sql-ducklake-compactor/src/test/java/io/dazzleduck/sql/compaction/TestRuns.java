package io.dazzleduck.sql.compaction;

import java.time.Instant;

/** Shared factory for {@link CompactionRun} fixtures in the compactor tests. */
final class TestRuns {
    private TestRuns() {}

    static CompactionRun run(long id, long startSec, long endSec, long gapMs,
                             Long filesBefore, Long filesAfter, Long bytesBefore, Long bytesAfter,
                             CompactionRun.Outcome outcome, CompactionRun.FailureClass failureClass) {
        Long retired = (filesBefore != null && filesAfter != null) ? filesBefore - filesAfter : null;
        return new CompactionRun(
                id, "db", "tier",
                Instant.ofEpochSecond(startSec), Instant.ofEpochSecond(startSec), Instant.ofEpochSecond(endSec),
                60_000, gapMs,
                filesBefore, filesAfter, retired, bytesBefore, bytesAfter,
                10, null,
                (endSec - startSec) * 1000, (endSec - startSec) * 1000, -1,
                outcome, failureClass, failureClass == CompactionRun.FailureClass.NONE ? null : "err",
                -1, -1, -1, CompactionRun.DEFAULT_COMMIT_TIMEOUT_MS);
    }
}
