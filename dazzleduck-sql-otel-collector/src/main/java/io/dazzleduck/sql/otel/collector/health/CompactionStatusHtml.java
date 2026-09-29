package io.dazzleduck.sql.otel.collector.health;

import io.dazzleduck.sql.otel.collector.compaction.CollectorCompactor;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** The compaction section of {@code /stats}: one row per catalog and job, using the page's styles. */
public final class CompactionStatusHtml {

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    private CompactionStatusHtml() {
    }

    public static String render(CollectorCompactor.Status status) {
        StringBuilder html = new StringBuilder("<h1 style=\"margin-top:24px\">Compaction</h1>");
        if (!status.enabled()) {
            return html.append("<div class=\"table-wrapper\"><div class=\"empty\">")
                    .append("Compaction is disabled (otel_collector.compaction.enabled = false)")
                    .append("</div></div>").toString();
        }
        html.append("<div class=\"table-wrapper\"><table><thead><tr>")
                .append("<th>Catalog</th><th>Job</th><th>Last run</th><th>Next run</th><th>Outcome</th>")
                .append("<th>Duration</th><th>Merged (last / total)</th><th>Rewritten (last / total)</th>")
                .append("<th>Runs</th><th>Failed runs</th><th>Snapshots</th><th>Last error</th>")
                .append("</tr></thead><tbody>");
        Instant now = Instant.now();
        for (var job : status.jobs()) {
            Long snapshots = status.snapshotCounts().get(job.catalog());
            html.append("<tr>")
                    .append(cell(job.catalog())).append(cell(job.job()))
                    .append(cell(job.lastStart() == null ? "not run yet" : TIME.format(job.lastStart())))
                    .append(cell(job.nextRun() == null ? "" : TIME.format(job.nextRun())
                            + " (in " + humanize(Duration.between(now, job.nextRun())) + ")"))
                    .append(outcomeCell(job.lastOutcome()))
                    .append(cell(job.lastStart() == null ? "" : job.lastDurationMs() + " ms"))
                    .append(cell(job.lastFilesMerged() + " / " + job.totalFilesMerged()))
                    .append(cell(job.lastFilesRewritten() + " / " + job.totalFilesRewritten()))
                    .append(cell(Long.toString(job.runs())))
                    .append(job.failedRuns() > 0 ? "<td class=\"warn\">" + job.failedRuns() + "</td>" : cell("0"))
                    .append(cell(snapshots == null || snapshots < 0 ? "?" : Long.toString(snapshots)))
                    .append(cell(job.lastError() == null ? "" : job.lastError()))
                    .append("</tr>");
        }
        return html.append("</tbody></table></div>").toString();
    }

    private static String outcomeCell(CollectorCompactor.Outcome outcome) {
        if (outcome == null) return cell("");
        return switch (outcome) {
            case OK -> cell("OK");
            case CONFLICT -> "<td class=\"warn\">conflict (retried next run)</td>";
            case FAILED -> "<td class=\"bad\">failed</td>";
        };
    }

    private static String humanize(Duration d) {
        long s = Math.max(0, d.toSeconds());
        if (s < 120) return s + "s";
        if (s < 7200) return (s / 60) + "m";
        return (s / 3600) + "h";
    }

    private static String cell(String text) {
        return "<td>" + escape(text) + "</td>";
    }

    static String escape(String text) {
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}
