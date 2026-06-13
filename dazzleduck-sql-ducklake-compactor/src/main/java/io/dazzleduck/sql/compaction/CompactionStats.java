package io.dazzleduck.sql.compaction;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public record CompactionStats(
        Instant serviceStart,
        Map<String, DatabaseStats> databases) {

    public record DatabaseStats(
            long totalMinorCompactions,
            long totalMajorCompactions,
            long totalFilesCompacted,
            Instant lastExecutionTime,
            Instant nextExecutionTime,
            long currentSmallFiles,
            long currentMediumFiles,
            long currentTotalFiles) {}

    public Duration uptime() {
        return Duration.between(serviceStart, Instant.now());
    }
}
