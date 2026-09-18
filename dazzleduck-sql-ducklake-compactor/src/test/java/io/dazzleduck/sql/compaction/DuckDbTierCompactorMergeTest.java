package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies files_processed / files_created extraction against a real DuckLake merge, using a DuckDB
 * file as the catalog so no Postgres/Testcontainers is needed. Confirms
 * {@code ducklake_merge_adjacent_files} returns {@code (schema_name, table_name, files_processed,
 * files_created)} and that {@link DuckDbTierCompactor} sums both.
 */
class DuckDbTierCompactorMergeTest {

    @TempDir
    Path tmp;

    private static String startupScript(Path catalog, Path data) {
        // Newline after each ';' so RawConnections.splitStatements runs them individually.
        // Idempotent (INSTALL/LOAD/ATTACH/CALL set_option are all replay-safe), as RawConnections requires.
        return "INSTALL ducklake;\n"
                + "LOAD ducklake;\n"
                + "ATTACH 'ducklake:" + catalog + "' AS lake (DATA_PATH '" + data + "/');\n"
                + "CALL lake.set_option('data_inlining_row_limit', 0);\n";
    }

    @Test
    void compactReportsFilesProcessed() throws Exception {
        Path catalog = tmp.resolve("catalog.ducklake");
        Path data = Files.createDirectories(tmp.resolve("data"));
        String startup = startupScript(catalog, data);

        // Setup: create the table and four parquet files, then close the instance so the DuckDB
        // catalog file's write lock is released before the compactor opens its own instance.
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
             Statement s = c.createStatement()) {
            s.execute("INSTALL ducklake");
            s.execute("LOAD ducklake");
            s.execute("ATTACH 'ducklake:" + catalog + "' AS lake (DATA_PATH '" + data + "/')");
            s.execute("CALL lake.set_option('data_inlining_row_limit', 0)");
            s.execute("CREATE TABLE lake.main.t (id INTEGER, v VARCHAR)");
            for (int i = 0; i < 4; i++) {
                s.execute("INSERT INTO lake.main.t SELECT i, 'x' || i FROM range(1, 1000) t(i)");
            }
        }

        CompactionState state = new CompactionState(new SimpleMeterRegistry(), List.of("lake"), List.of("all"));
        CompactionTier tier = new CompactionTier("all", true, Duration.ofSeconds(60), 0, 1_000_000_000L, 10, List.of());
        try (DuckDbTierCompactor compactor = new DuckDbTierCompactor(startup, state)) {
            TierCompactor.MergeOutcome out = compactor.compact("lake", tier);
            assertNotNull(out.filesProcessed(), "a merge happened, so counts are captured");
            assertEquals(4L, out.filesProcessed(), "4 input files compacted (files_processed)");
            assertEquals(1L, out.filesCreated(), "merged into 1 output file (files_created)");
            assertTrue(out.durationMergeMs() >= 0);

            // A second cycle finds a single merged file — nothing adjacent to merge -> no rows -> null.
            TierCompactor.MergeOutcome noop = compactor.compact("lake", tier);
            assertNull(noop.filesProcessed(), "nothing merged the second time");
            assertNull(noop.filesCreated());
        }
    }
}
