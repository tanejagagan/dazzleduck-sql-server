package io.dazzleduck.sql.otel.collector.health;

import io.dazzleduck.sql.otel.collector.compaction.CollectorCompactor;
import io.dazzleduck.sql.otel.collector.compaction.CollectorCompactor.JobStatus;
import io.dazzleduck.sql.otel.collector.compaction.CollectorCompactor.Outcome;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CompactionStatusHtmlTest {

    private static final Instant NOW = Instant.now();

    @Test
    void disabledSaysSoInsteadOfAnEmptyTable() {
        String html = CompactionStatusHtml.render(new CollectorCompactor.Status(false, List.of(), Map.of()));
        assertTrue(html.contains("Compaction is disabled"), html);
        assertFalse(html.contains("<table>"), html);
    }

    @Test
    void rowsShowEachJobsLastRun() {
        var minor = new JobStatus("lake", "minor", NOW.minusSeconds(30), 42, Outcome.OK, null,
                10, 0, 250, 0, 7, 0, NOW.plusSeconds(30));
        var major = new JobStatus("lake", "major", NOW.minusSeconds(600), 900, Outcome.FAILED,
                "rewrite_deletes: <disk full> & more", 0, 0, 12, 3, 2, 1, NOW.plus(Duration.ofMinutes(50)));
        var neverRun = new JobStatus("lake", "orphan_cleanup", null, 0, null, null, 0, 0, 0, 0, 0, 0,
                NOW.plus(Duration.ofHours(24)));
        String html = CompactionStatusHtml.render(new CollectorCompactor.Status(true,
                List.of(minor, major, neverRun), Map.of("lake", 5L)));

        assertTrue(html.contains("<td>minor</td>"), html);
        assertTrue(html.contains("<td>10 / 250</td>"), "merged last / total");
        assertTrue(html.contains("<td>0 / 3</td>"), "rewritten last / total");
        assertTrue(html.contains("<td>42 ms</td>"), html);
        assertTrue(html.contains("<td class=\"bad\">failed</td>"), html);
        assertTrue(html.contains("<td class=\"warn\">1</td>"), "failed runs highlighted");
        assertTrue(html.contains("<td>5</td>"), "snapshot count");
        assertTrue(html.contains("not run yet"), html);
        assertTrue(html.contains("rewrite_deletes: &lt;disk full&gt; &amp; more"), "errors are HTML-escaped");
        assertFalse(html.contains("<disk full>"), html);
    }

    @Test
    void anUnreadableSnapshotCountShowsAQuestionMark() {
        var minor = new JobStatus("lake", "minor", null, 0, null, null, 0, 0, 0, 0, 0, 0, NOW);
        String html = CompactionStatusHtml.render(new CollectorCompactor.Status(true, List.of(minor), Map.of("lake", -1L)));
        assertTrue(html.contains("<td>?</td>"), html);
    }
}
