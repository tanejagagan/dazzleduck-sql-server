package io.dazzleduck.sql.compaction;

import java.time.Duration;
import java.util.List;

/**
 * One configured compaction level (what used to be hardcoded as "minor" and "major"). Any number of
 * tiers can be configured, as long as enabled tiers' {@code [minFileSize, maxFileSize)} ranges never
 * overlap — validated once at startup in {@link CompactionConfig#from}, which is what lets tiers run
 * concurrently with no lock between them.
 */
public record CompactionTier(
        String name,
        boolean enabled,
        Duration frequency,
        long minFileSize,
        long maxFileSize,
        long maxCompactedFiles,
        List<String> connectionSettings
) {
}
