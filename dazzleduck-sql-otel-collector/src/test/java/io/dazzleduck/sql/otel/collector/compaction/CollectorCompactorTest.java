package io.dazzleduck.sql.otel.collector.compaction;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs each job against a DuckDB-file DuckLake catalog attached to the shared {@link ConnectionPool}
 * instance — the setup this compactor exists for, since such a catalog can only be attached once per
 * process.
 */
class CollectorCompactorTest {

    private static final AtomicInteger SEQUENCE = new AtomicInteger();

    @TempDir
    Path tempDir;

    private String catalog;
    private String metadata;
    private Path dataPath;
    private SimpleMeterRegistry registry;

    @BeforeEach
    void attachCatalog() throws Exception {
        catalog = "cc_lake_" + SEQUENCE.incrementAndGet();
        metadata = "__ducklake_metadata_" + catalog;
        dataPath = tempDir.resolve("data");
        Files.createDirectories(dataPath);
        registry = new SimpleMeterRegistry();
        ConnectionPool.execute("ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0)"
                .formatted(tempDir.resolve("meta.ducklake"), catalog, dataPath));
        ConnectionPool.execute("CREATE TABLE %s.main.t (id BIGINT, v VARCHAR)".formatted(catalog));
        // Ten small files, as ingestion leaves them.
        for (int i = 0; i < 10; i++) {
            ConnectionPool.execute("INSERT INTO %s.main.t SELECT range + %d, 'row' FROM range(10)".formatted(catalog, i * 10));
        }
    }

    @AfterEach
    void detachCatalog() throws Exception {
        ConnectionPool.execute("DETACH " + catalog);
    }

    private CompactionSettings settings(Duration retention, Duration orphanAge) {
        return new CompactionSettings(true, List.of(catalog), Duration.ofMillis(100), 8L * 1024 * 1024,
                Duration.ofHours(1), retention, 0.3, true, Duration.ofDays(1), orphanAge);
    }

    private long scalar(String sql) throws Exception {
        return ConnectionPool.collectFirst(sql, Long.class);
    }

    private long activeDataFiles() throws Exception {
        return scalar("SELECT count(*) FROM %s.ducklake_data_file WHERE end_snapshot IS NULL".formatted(metadata));
    }

    private long activeDeleteFiles() throws Exception {
        return scalar("SELECT count(*) FROM %s.ducklake_delete_file WHERE end_snapshot IS NULL".formatted(metadata));
    }

    private long parquetFilesInStorage() throws Exception {
        try (var files = Files.walk(dataPath)) {
            return files.filter(p -> p.toString().endsWith(".parquet")).count();
        }
    }

    private double counter(String name) {
        var c = registry.find("dazzleduck.otel.compaction." + name).tag("catalog", catalog).counter();
        return c == null ? 0 : c.count();
    }

    /** A Parquet file the catalog does not reference: a batch the collector wrote but has not registered. */
    private Path unregisteredFile(String name) throws Exception {
        Path tableDir = dataPath.resolve("main").resolve("t");
        Files.createDirectories(tableDir);
        Path file = tableDir.resolve(name);
        ConnectionPool.execute("COPY (SELECT 1 AS id, 'x' AS v) TO '%s' (FORMAT parquet)".formatted(file));
        return file;
    }

    @Test
    void minorMergesSmallFiles() throws Exception {
        assertEquals(10, activeDataFiles());
        new CollectorCompactor(settings(Duration.ofMinutes(15), Duration.ofDays(2)), registry).runMinor();

        assertEquals(1, activeDataFiles());
        assertEquals(10.0, counter("files_merged"));
        assertEquals(100, scalar("SELECT count(*) FROM %s.main.t".formatted(catalog)));
    }

    @Test
    void majorRewritesExpiresAndDeletesRetiredFilesButNeverUnregisteredOnes() throws Exception {
        var compactor = new CollectorCompactor(settings(Duration.ZERO, Duration.ofDays(2)), registry);
        compactor.runMinor();
        ConnectionPool.execute("DELETE FROM %s.main.t WHERE id < 50".formatted(catalog));
        assertEquals(1, activeDeleteFiles());
        Path inFlight = unregisteredFile("in_flight_batch.parquet");
        long newestBefore = scalar("SELECT max(snapshot_id) FROM %s.ducklake_snapshot".formatted(metadata));

        compactor.runMajor();

        assertEquals(0, activeDeleteFiles(), "50% deleted is over the 0.3 threshold, so the file is rewritten");
        assertEquals(1.0, counter("files_rewritten"));
        // Expiry runs before the merge and rewrite, which then add snapshots of their own; what must be
        // gone is every earlier snapshot except the newest, which DuckLake never expires.
        assertEquals(0, scalar("SELECT count(*) FROM %s.ducklake_snapshot WHERE snapshot_id < %d"
                .formatted(metadata, newestBefore)), "with zero retention older snapshots are expired");
        // Same order as DuckLake's CHECKPOINT: expire, merge/rewrite, clean up. The ten files the minor
        // merge retired are gone; the data and delete file this run's rewrite retired are still in a
        // snapshot that existed when expiry ran, so the next major run frees them.
        assertEquals(activeDataFiles() + 1 + 2, parquetFilesInStorage(),
                "live file, the unregistered file, and the two files this run's rewrite retired");
        compactor.runMajor();
        assertEquals(activeDataFiles() + 1, parquetFilesInStorage(),
                "after the next run only live files and the unregistered one remain");
        assertTrue(Files.exists(inFlight), "a major run must never delete a file the catalog does not know");
        assertEquals(50, scalar("SELECT count(*) FROM %s.main.t".formatted(catalog)));
        assertEquals(50, scalar("SELECT min(id) FROM %s.main.t".formatted(catalog)));
    }

    @Test
    void orphanCleanupDeletesOnlyFilesOlderThanTheConfiguredAge() throws Exception {
        Path recent = unregisteredFile("recent_batch.parquet");
        Path stale = unregisteredFile("stale_batch.parquet");
        Files.setLastModifiedTime(stale, FileTime.from(Instant.now().minus(Duration.ofDays(3))));

        new CollectorCompactor(settings(Duration.ofMinutes(15), Duration.ofDays(2)), registry).runOrphanCleanup();

        assertTrue(Files.exists(recent), "younger than older_than: kept");
        assertFalse(Files.exists(stale), "older than older_than: deleted");
        assertEquals(10, activeDataFiles(), "registered files are untouched");
    }

    @Test
    void aFailingStepIsCountedAndDoesNotThrow() {
        var settings = new CompactionSettings(true, List.of("no_such_catalog"), Duration.ofMinutes(1), 1024,
                Duration.ofHours(1), Duration.ofMinutes(15), null, false, Duration.ofDays(1), Duration.ofDays(2));
        assertDoesNotThrow(() -> new CollectorCompactor(settings, registry).runMinor());
        var failures = registry.find("dazzleduck.otel.compaction.failures")
                .tag("catalog", "no_such_catalog").tag("step", "minor_merge").counter();
        assertNotNull(failures);
        assertEquals(1.0, failures.count());
    }

    @Test
    void startSchedulesTheJobsAndCloseStopsThem() throws Exception {
        var compactor = new CollectorCompactor(settings(Duration.ofMinutes(15), Duration.ofDays(2)), registry);
        compactor.start();
        long deadline = System.currentTimeMillis() + 10_000;
        while (counter("files_merged") < 10 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        compactor.close();
        assertEquals(10.0, counter("files_merged"), "the minor job ran on its schedule");
        assertEquals(1, activeDataFiles());
    }

    @Test
    void startIsANoOpWhenDisabled() throws Exception {
        var compactor = new CollectorCompactor(CompactionSettings.disabled(), registry);
        compactor.start();
        Thread.sleep(300);
        compactor.close();
        assertEquals(10, activeDataFiles());
        assertNull(registry.find("dazzleduck.otel.compaction.duration").timer());
    }
}
