package io.dazzleduck.sql.commons.ingestion;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class StatsHtmlTest {

    private Stats leaf(String id, long rows, long rejected429) {
        return Stats.builder(id).rowsWritten(rows).totalWriteBytes(rows * 10)
                .rejected429(rejected429).build();
    }

    @Test
    public void emptyListRendersPlaceholderRow() {
        String html = StatsHtml.renderTable(List.of(), "Queues");
        assertTrue(html.contains("No active ingestion queues"), html);
        assertTrue(html.contains("<caption"), html);
    }

    @Test
    public void rendersRowPerQueueWithAllColumns() {
        String html = StatsHtml.renderTable(List.of(leaf("logs", 100, 0), leaf("traces", 50, 3)), null);
        assertTrue(html.contains("logs"));
        assertTrue(html.contains("traces"));
        // Headers for the new counters are present.
        assertTrue(html.contains("Rej 429"));
        assertTrue(html.contains("Rej multi-part"));
        assertTrue(html.contains("Rows"));
        assertTrue(html.contains("Last write"));
        // A non-zero 429 count is flagged with the warn class.
        assertTrue(html.contains("class=\"warn\">3</td>"), html);
    }

    @Test
    public void partitionedQueueRendersIndentedChildRows() {
        Stats partitioned = Stats.builder("events")
                .rowsWritten(300)
                .rejectedMultiPartition(2)
                .partitions(List.of(leaf("p0", 100, 0), leaf("p1", 200, 0)))
                .build();
        String html = StatsHtml.renderTable(List.of(partitioned), null);
        assertTrue(html.contains("class=\"partition\""), "expected partition child rows");
        assertTrue(html.contains("↳ p0"), html);
        assertTrue(html.contains("↳ p1"), html);
        // Multi-partition rejections flagged as bad on the parent row.
        assertTrue(html.contains("class=\"bad\">2</td>"), html);
    }

    @Test
    public void pageIsSelfContainedAndAutoRefreshes() {
        String page = StatsHtml.renderPage(List.of(leaf("logs", 1, 0)), "Collector Stats", 5);
        assertTrue(page.startsWith("<!DOCTYPE html>"));
        assertTrue(page.contains("http-equiv=\"refresh\" content=\"5\""), page);
        assertTrue(page.contains("Collector Stats"));
        assertTrue(page.contains("<table>"));
    }

    @Test
    public void ageFormatsHumanReadable() {
        long now = 1_000_000_000L;
        assertEquals("—", StatsHtml.age(0, now));
        assertEquals("—", StatsHtml.age(now + 5000, now)); // future
        assertEquals("5s", StatsHtml.age(now - 5_000, now));
        assertEquals("3m", StatsHtml.age(now - 200_000, now));
        assertEquals("2h", StatsHtml.age(now - 7_200_000, now));
    }

    @Test
    public void perMinuteSeriesRenderAsSparklines() {
        Stats s = Stats.builder("logs")
                .rowsWrittenPerMinute(new long[]{10, 20, 40, 30, 60})
                .batchesReceivedPerMinute(new long[]{1, 2, 3, 2, 4})
                .build();
        String html = StatsHtml.renderTable(List.of(s), null);
        assertTrue(html.contains("Rows/min (15m)"));
        assertTrue(html.contains("Arrivals/min (15m)"));
        assertTrue(html.contains("<svg"), "expected an inline sparkline svg");
        assertTrue(html.contains("<polyline"));
        assertTrue(html.contains("rows/min — peak 60/min"), html);
    }

    @Test
    public void emptyOrAllZeroSeriesRenderDash() {
        Stats none = Stats.builder("q").build(); // empty arrays
        Stats zero = Stats.builder("q2").rowsWrittenPerMinute(new long[]{0, 0, 0}).build();
        assertFalse(StatsHtml.renderTable(List.of(none), null).contains("<svg"), "empty series -> no svg");
        String zeroHtml = StatsHtml.renderTable(List.of(zero), null);
        assertTrue(zeroHtml.contains("no activity"), zeroHtml);
    }

    @Test
    public void escapesHtmlInIdentifierAndError() {
        Stats s = Stats.builder("<script>").lastError("<bad> & \"stuff\"").lastErrorEpochMs(1).build();
        String html = StatsHtml.renderTable(List.of(s), null);
        assertFalse(html.contains("<script>"), "identifier must be escaped");
        assertTrue(html.contains("&lt;script&gt;"));
        assertTrue(html.contains("&lt;bad&gt;"));
    }
}
