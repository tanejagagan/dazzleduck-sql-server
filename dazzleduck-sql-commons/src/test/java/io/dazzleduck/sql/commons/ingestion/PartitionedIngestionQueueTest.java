package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.MutableClock;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;

public class PartitionedIngestionQueueTest {
    private static final long DEFAULT_MIN_BATCH_SIZE = 10 * 1024;
    private static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(5);
    private static final String TEST_APP_ID = "test-app";
    private static final String INPUT_FORMAT = "parquet";
    private static final Pattern SHARD_FILENAME = Pattern.compile("dd_shard(\\d+)_");

    @TempDir
    Path tempDir;

    private Path sourceFile;
    private Path targetPath;

    @BeforeEach
    public void setup() throws Exception {
        sourceFile = createTestParquetFile("source1.parquet", 100);
        targetPath = tempDir.resolve("output");
        Files.createDirectories(targetPath);
    }

    @AfterEach
    public void cleanup() throws Exception {
        // Mirrors ParquetIngestionQueueTest: let abandoned writer threads quiesce before @TempDir
        // deletion starts, since an in-flight shard COPY racing against directory cleanup fails.
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline && writerThreadsAlive()) {
            Thread.sleep(20);
        }
    }

    private static boolean writerThreadsAlive() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.isAlive() && t.getName().startsWith("BulkIngestQueue-"));
    }

    @Test
    public void testPartitionedRoutingIsDisjointAndComplete() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var handler = createHandler(new AtomicInteger(), null);
        int parallelWriters = 3;

        try (var queue = new PartitionedIngestionQueue(
                TEST_APP_ID, INPUT_FORMAT, targetPath.toString(), "test-queue",
                DEFAULT_MIN_BATCH_SIZE, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                DEFAULT_MAX_DELAY, null, handler, service, clock, "id", parallelWriters)) {

            var future = queue.add(createBatch(sourceFile.toString(), "producer1", 0, DEFAULT_MIN_BATCH_SIZE + 1));
            service.tick(1, TimeUnit.MILLISECONDS);
            var result = future.get(5, SECONDS);

            assertEquals(100, result.rowCount());
            assertEquals(parallelWriters, result.filesCreated().size(), "one unpartitioned output file per shard");

            long totalRowsAcrossFiles = 0;
            for (String file : result.filesCreated()) {
                Matcher m = SHARD_FILENAME.matcher(file);
                assertTrue(m.find(), "expected shard index encoded in filename: " + file);
                int shard = Integer.parseInt(m.group(1));

                long mismatched = ConnectionPool.collectFirst(
                        "SELECT count(*) FROM read_parquet('%s') WHERE hash(id) %% %d <> %d"
                                .formatted(file, parallelWriters, shard),
                        Long.class);
                assertEquals(0L, mismatched, "shard " + shard + "'s file must contain only its own hash bucket");

                totalRowsAcrossFiles += ConnectionPool.collectFirst(
                        "SELECT count(*) FROM read_parquet('%s')".formatted(file), Long.class);
            }
            assertEquals(100L, totalRowsAcrossFiles, "no row lost or duplicated across shard files");

            String unionAll = result.filesCreated().stream().map("'%s'"::formatted).collect(Collectors.joining(","));
            long distinctIds = ConnectionPool.collectFirst(
                    "SELECT count(DISTINCT id) FROM read_parquet([%s])".formatted(unionAll), Long.class);
            assertEquals(100L, distinctIds);
        }
    }

    @Test
    public void testSingleWriterDegradesToBaseBehavior() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var handler = createHandler(new AtomicInteger(), null);

        try (var queue = new PartitionedIngestionQueue(
                TEST_APP_ID, INPUT_FORMAT, targetPath.toString(), "test-queue",
                DEFAULT_MIN_BATCH_SIZE, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                DEFAULT_MAX_DELAY, null, handler, service, clock, null, 1)) {

            var future = queue.add(createBatch(sourceFile.toString(), "producer1", 0, DEFAULT_MIN_BATCH_SIZE + 1));
            service.tick(1, TimeUnit.MILLISECONDS);
            var result = future.get(5, SECONDS);

            assertEquals(100, result.rowCount());
            assertEquals(1, result.filesCreated().size());
            assertFalse(result.filesCreated().get(0).contains("dd_shard"),
                    "single-writer path must fall back to the exact base ParquetIngestionQueue behavior");
        }
    }

    @Test
    public void testAllShardsFailBeforeAnyCommit() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var commitCount = new AtomicInteger(0);
        // A column that doesn't exist makes every shard's hash(...) filter fail identically —
        // a simple, deterministic way to force a whole-batch COPY failure across all shards.
        var handler = createHandler(commitCount, null);

        try (var queue = new PartitionedIngestionQueue(
                TEST_APP_ID, INPUT_FORMAT, targetPath.toString(), "test-queue",
                DEFAULT_MIN_BATCH_SIZE, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                DEFAULT_MAX_DELAY, null, handler, service, clock, "no_such_column", 3)) {

            var future = queue.add(createBatch(sourceFile.toString(), "producer1", 0, DEFAULT_MIN_BATCH_SIZE + 1));
            service.tick(1, TimeUnit.MILLISECONDS);

            var thrown = assertThrows(Exception.class, () -> future.get(5, SECONDS));
            String message = thrown.getCause() != null ? thrown.getCause().getMessage() : thrown.getMessage();
            assertNotNull(message);
            assertTrue(message.contains("3 of 3 shard(s) failed"), "message must name how many/of how many shards failed: " + message);
            assertTrue(message.contains("shard 0 [hash"), "message must name the failing shard and its routing filter: " + message);
            assertTrue(message.contains("no_such_column"), "message must surface the underlying per-shard error: " + message);
            assertEquals(0, commitCount.get(), "no shard's post-ingestion task may run when any shard's COPY fails");
        }
    }

    @Test
    public void testPartialFailureNotifiesOnlyAffectedBatches() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        int parallelWriters = 3;
        int failingShard = 1;

        // Partition a range of ids by which shard they route to, so the "unaffected" and
        // "affected" batches below are built from real hash(id) % N routing, not guesswork.
        List<Long> unaffectedIds = new ArrayList<>();
        List<Long> affectedIds = new ArrayList<>();
        try (var conn = ConnectionPool.getConnection();
             var stmt = conn.createStatement();
             var rs = stmt.executeQuery("SELECT range AS id, hash(range) %% %d AS shard FROM range(0, 50)".formatted(parallelWriters))) {
            while (rs.next()) {
                (rs.getLong("shard") == failingShard ? affectedIds : unaffectedIds).add(rs.getLong("id"));
            }
        }
        assertFalse(unaffectedIds.isEmpty());
        assertFalse(affectedIds.isEmpty());

        Path unaffectedFile = writeIdRows("unaffected.parquet", unaffectedIds);
        // Only needs to contain at least one row landing in the failing shard.
        Path affectedFile = writeIdRows("affected.parquet", affectedIds.subList(0, 1));

        var commitCount = new AtomicInteger();
        var handler = new IngestionHandler() {
            @Override
            public PostIngestionTask createPostIngestionTask(IngestionResult ingestionResult) {
                boolean isFailingShard = ingestionResult.filesCreated().stream()
                        .anyMatch(f -> f.contains("dd_shard" + failingShard + "_"));
                return () -> {
                    if (isFailingShard) {
                        throw new RuntimeException("simulated catalog failure for shard " + failingShard);
                    }
                    commitCount.incrementAndGet();
                };
            }

            @Override
            public String getTargetPath(String queueId) { return null; }

            @Override
            public String[] getPartitionBy(String queueId) { return new String[0]; }
        };

        try (var queue = new PartitionedIngestionQueue(
                TEST_APP_ID, INPUT_FORMAT, targetPath.toString(), "test-queue",
                DEFAULT_MIN_BATCH_SIZE, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                DEFAULT_MAX_DELAY, null, handler, service, clock, "id", parallelWriters)) {

            // Both batches must land in the SAME flushed bucket for this to exercise attribution:
            // the first stays under minBucketSize, the second pushes the bucket over it, so add()
            // flushes both together in one write().
            var unaffectedFuture = queue.add(createBatch(unaffectedFile.toString(), "producer-safe", 0, DEFAULT_MIN_BATCH_SIZE / 2));
            var affectedFuture = queue.add(createBatch(affectedFile.toString(), "producer-affected", 0, DEFAULT_MIN_BATCH_SIZE + 1));
            service.tick(1, TimeUnit.MILLISECONDS);

            // The unaffected producer's batch succeeds even though another batch in the same flush
            // was routed (in part) to a shard whose commit failed.
            var result = unaffectedFuture.get(5, SECONDS);
            assertTrue(result.rowCount() > 0);

            // The affected producer's batch fails, naming exactly which shard/filter it hit.
            var thrown = assertThrows(Exception.class, () -> affectedFuture.get(5, SECONDS));
            String message = thrown.getCause() != null ? thrown.getCause().getMessage() : thrown.getMessage();
            assertNotNull(message);
            assertTrue(message.contains("routed to a failed shard"), message);
            assertTrue(message.contains("shard " + failingShard + " ["), message);

            // Every shard except the failing one committed (2 of 3).
            assertEquals(parallelWriters - 1, commitCount.get());
        }
    }

    @Test
    public void testWatermarkRowsComputedPerShard() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var watermarkSpec = new WatermarkSpec("wm_table", "value", List.of("category"),
                "min_value", "max_value", "row_count", "snapshot_id");
        var handler = createHandler(new AtomicInteger(), watermarkSpec);

        try (var queue = new PartitionedIngestionQueue(
                TEST_APP_ID, INPUT_FORMAT, targetPath.toString(), "test-queue",
                DEFAULT_MIN_BATCH_SIZE, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                DEFAULT_MAX_DELAY, null, handler, service, clock, "id", 3)) {

            var future = queue.add(createBatch(sourceFile.toString(), "producer1", 0, DEFAULT_MIN_BATCH_SIZE + 1));
            service.tick(1, TimeUnit.MILLISECONDS);
            var result = future.get(5, SECONDS);

            assertNotNull(result.watermarkRows());
            assertFalse(result.watermarkRows().isEmpty());
            // Row-count column is last (groups, MIN, MAX, COUNT) — summed across every shard's own
            // per-group aggregation, it must still account for every row exactly once.
            long totalCounted = result.watermarkRows().stream()
                    .mapToLong(row -> Long.parseLong(row.get(row.size() - 1)))
                    .sum();
            assertEquals(100L, totalCounted, "sum of per-shard, per-group row counts must equal total rows written");
        }
    }

    private Path writeIdRows(String filename, List<Long> ids) throws Exception {
        Path file = tempDir.resolve(filename);
        String idList = ids.stream().map(String::valueOf).collect(Collectors.joining(","));
        ConnectionPool.execute("COPY (SELECT UNNEST([%s]) AS id) TO '%s' (FORMAT PARQUET)".formatted(idList, file));
        return file;
    }

    private Path createTestParquetFile(String filename, int rowCount) throws Exception {
        Path file = tempDir.resolve(filename);
        String sql = "COPY (SELECT i AS id, i * 2 AS value, 'category' || (i %% 3) AS category FROM range(0, %d) t(i)) TO '%s' (FORMAT PARQUET)"
                .formatted(rowCount, file);
        ConnectionPool.execute(sql);
        return file;
    }

    private Batch<String> createBatch(String file, String producerId, long batchId, long totalSize) {
        return new Batch<>(null, null, file, producerId, batchId, totalSize, "parquet", Instant.now());
    }

    private IngestionHandler createHandler(AtomicInteger commitCount, WatermarkSpec watermarkSpec) {
        return new IngestionHandler() {
            @Override
            public PostIngestionTask createPostIngestionTask(IngestionResult ingestionResult) {
                return commitCount::incrementAndGet;
            }

            @Override
            public String getTargetPath(String queueId) { return null; }

            @Override
            public String[] getPartitionBy(String queueId) { return new String[0]; }

            @Override
            public WatermarkSpec getWatermarkSpec(String queueId) { return watermarkSpec; }
        };
    }
}
