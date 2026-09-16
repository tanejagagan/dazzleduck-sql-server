package io.dazzleduck.sql.compaction;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public record CompactionStats(
        Instant serviceStart,
        Map<String, DatabaseStats> databases) {

    /** Compaction counts are successes only; {@code lastSuccessTime} is null until one completes. */
    public record DatabaseStats(
            Map<String, Long> tierCompactionCounts,
            long totalFailedCycles,
            long totalFilesCompacted,
            Instant lastSuccessTime,
            Map<String, Instant> nextExecutionTimeByTier,
            Map<String, Long> currentTierFileCounts,
            long currentTotalFiles) {

        /**
         * Fills in the one field {@link CompactionState} cannot know, so the seven-component
         * constructor is spelled out in exactly one place.
         */
        public DatabaseStats withNextExecutionTimeByTier(Map<String, Instant> next) {
            return new DatabaseStats(tierCompactionCounts, totalFailedCycles,
                    totalFilesCompacted, lastSuccessTime, next,
                    currentTierFileCounts, currentTotalFiles);
        }
    }

    public Duration uptime() {
        return Duration.between(serviceStart, Instant.now());
    }
}
