package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CompactionHtmlTest {

    @Test
    void emptyLogRendersPlaceholderPage() {
        String html = CompactionHtml.renderPage(new CompactionRunLog(50), 5);
        assertTrue(html.startsWith("<!DOCTYPE html>"));
        assertTrue(html.contains("http-equiv=\"refresh\" content=\"5\""));
        assertTrue(html.contains("No compaction cycles recorded yet"));
    }

    @Test
    void rendersCardWithOutcomeBadgeAndSparklines() {
        CompactionRunLog log = new CompactionRunLog(50);
        log.record(TestRuns.run(1, 0, 2, 5000, 100L, 20L, 1000L, 200L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        log.record(TestRuns.run(2, 10, 13, 5000, 50L, 10L, 500L, 100L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        String html = CompactionHtml.renderPage(log, 5);

        assertTrue(html.contains("b-success"), "outcome badge");
        assertTrue(html.contains("tier"), "tier name");
        assertTrue(html.contains("<svg"), "sparklines rendered");
        assertTrue(html.contains("throughput"));
        assertTrue(html.contains("drain"));
        assertTrue(html.contains("headroom"));
    }

    @Test
    void failedCardShowsClassChipAndError() {
        CompactionRunLog log = new CompactionRunLog(50);
        log.record(TestRuns.run(1, 0, 1, 100, null, null, null, null,
                CompactionRun.Outcome.FAILED, CompactionRun.FailureClass.OUT_OF_MEMORY));
        String html = CompactionHtml.renderPage(log, 5);
        assertTrue(html.contains("b-failed"));
        assertTrue(html.contains("OUT_OF_MEMORY"));
    }

    @Test
    void escapesHtmlInErrorMessage() {
        CompactionRunLog log = new CompactionRunLog(50);
        CompactionRun run = new CompactionRun(1, "db", "tier",
                java.time.Instant.now(), java.time.Instant.now(), java.time.Instant.now(),
                60000, 100, null, null, null, null, null, 10, null, 5, 5,
                CompactionRun.Outcome.FAILED, CompactionRun.FailureClass.OTHER, "<script>alert(1)</script>",
                -1, -1, -1, CompactionRun.DEFAULT_COMMIT_TIMEOUT_MS);
        log.record(run);
        String html = CompactionHtml.renderPage(log, 5);
        assertFalse(html.contains("<script>alert(1)</script>"));
        assertTrue(html.contains("&lt;script&gt;"));
    }
}
