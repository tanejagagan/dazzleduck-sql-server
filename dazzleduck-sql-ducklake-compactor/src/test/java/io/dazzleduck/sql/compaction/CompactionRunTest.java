package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.Test;

import static io.dazzleduck.sql.compaction.CompactionRun.FailureClass.*;
import static org.junit.jupiter.api.Assertions.*;

class CompactionRunTest {

    @Test
    void classifiesCommitTimeout() {
        assertEquals(COMMIT_TIMEOUT, CompactionRun.classify(
                new RuntimeException("Failed to execute query \"ROLLBACK\"")));
        assertEquals(COMMIT_TIMEOUT, CompactionRun.classify(
                new RuntimeException("connection died: idle_in_transaction_session_timeout exceeded")));
    }

    @Test
    void classifiesTheOtherFamilies() {
        assertEquals(TRANSACTION_CONFLICT, CompactionRun.classify(new RuntimeException("Transaction conflict on table x")));
        assertEquals(OUT_OF_MEMORY, CompactionRun.classify(new RuntimeException("Out of Memory Error: failed to allocate")));
        assertEquals(SPILL_EXCEEDED, CompactionRun.classify(new RuntimeException("failed to offload data block to disk")));
        assertEquals(CATALOG_UNAVAILABLE, CompactionRun.classify(new RuntimeException("Connection refused to catalog")));
        assertEquals(OTHER, CompactionRun.classify(new RuntimeException("something unexpected")));
    }

    @Test
    void classifyWalksTheCauseChain() {
        Throwable t = new RuntimeException("wrapper", new IllegalStateException("Out of Memory Error"));
        assertEquals(OUT_OF_MEMORY, CompactionRun.classify(t));
    }

    @Test
    void truncatesLongErrorToRootCause() {
        String longMsg = "x".repeat(500);
        Throwable t = new RuntimeException("outer", new RuntimeException(longMsg));
        String truncated = CompactionRun.truncateError(t);
        assertTrue(truncated.length() <= 201, "should be ~200 chars + ellipsis");
        assertTrue(truncated.startsWith("x"));
        assertTrue(truncated.endsWith("…"));
    }

    @Test
    void bytesRetiredIsNullWhenEitherReadMissing() {
        assertNull(TestRuns.run(1, 0, 1, 0, 10L, 5L, null, 200L, CompactionRun.Outcome.SUCCESS, NONE).bytesRetired());
        assertEquals(800L, TestRuns.run(1, 0, 1, 0, 10L, 5L, 1000L, 200L, CompactionRun.Outcome.SUCCESS, NONE).bytesRetired());
    }
}
