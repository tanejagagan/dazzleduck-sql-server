package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.util.HeaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * A {@link ParquetIngestionQueue} that fans a single flushed bucket out across
 * {@code parallelWriters} concurrent {@code COPY} statements ("shards"), routing rows by
 * {@code hash(partitionColumn) % parallelWriters = shardIndex}. The accepting side (batching,
 * backpressure, producer-id dedup) is entirely unchanged — parallelism is applied only at flush
 * time, inside {@link #write}.
 *
 * <p><b>Commit ordering.</b> Each shard is its own independent DuckLake transaction (its own
 * {@link DuckLakePostIngestionTask} call), so there is no way to make all N shards commit
 * atomically without 2-phase-commit machinery DuckLake does not expose. To still guarantee "no
 * partial commit" for the common failure mode, every shard's {@code COPY} is run to completion
 * <b>before any</b> shard's post-ingestion task runs: {@link #writeAllShards} does the writes and
 * fails the whole batch (nothing committed) if any shard's COPY fails, and only once every shard
 * has written successfully does {@link #commitAllShards} register them with the catalog. A failure
 * during that second phase, after some shards already committed, is a genuine residual risk this
 * design cannot remove — it is logged loudly as a partial-commit anomaly rather than hidden.
 *
 * <p>No cross-shard merge of watermark rows or snapshot ids is needed: each shard computes its own
 * watermark rows over its own filtered relation, and its own {@link DuckLakePostIngestionTask}
 * predicts/verifies its own snapshot id independently, exactly as it would for any other ordinary
 * write — calling it N times (once per shard) is architecturally identical to N independent writes.
 */
public class PartitionedIngestionQueue extends ParquetIngestionQueue {

    private static final Logger logger = LoggerFactory.getLogger(PartitionedIngestionQueue.class);

    private final String partitionColumn;
    private final int parallelWriters;
    private final ExecutorService shardExecutor;

    public PartitionedIngestionQueue(String applicationId,
                                     String inputFormat,
                                     String outputPath,
                                     String ingestionQueue,
                                     long minBucketSize,
                                     long maxBucketSize,
                                     int maxBatches,
                                     long maxPendingWrite,
                                     Duration maxDelay,
                                     String parquetCompression,
                                     IngestionHandler postIngestionHandler,
                                     ScheduledExecutorService executorService,
                                     Clock clock,
                                     String partitionColumn,
                                     int parallelWriters) {
        super(applicationId, inputFormat, outputPath, ingestionQueue, minBucketSize, maxBucketSize, maxBatches,
                maxPendingWrite, maxDelay, parquetCompression, postIngestionHandler, executorService, clock);
        if (parallelWriters > 1 && (partitionColumn == null || partitionColumn.isBlank())) {
            throw new IllegalArgumentException(
                    "Queue '%s': parallelWriters=%d > 1 requires a non-blank partitionColumn"
                            .formatted(ingestionQueue, parallelWriters));
        }
        this.partitionColumn = partitionColumn;
        this.parallelWriters = Math.max(1, parallelWriters);
        this.shardExecutor = this.parallelWriters > 1
                ? Executors.newFixedThreadPool(this.parallelWriters, r -> {
                    Thread t = new Thread(r, "PartitionedIngestionQueue-" + ingestionQueue + "-shard");
                    t.setDaemon(true);
                    return t;
                })
                : null;
    }

    @Override
    public void write(WriteTask<String, IngestionResult> writeTask) {
        if (parallelWriters <= 1 || partitionColumn == null) {
            super.write(writeTask); // degrades to the exact unmodified single-shard behavior
            return;
        }
        logger.debug("Ingestion queue '{}' received batch with {} files, outputPath={}, parallelWriters={}",
                queueId, writeTask.bucket().batches().size(), outputPath, parallelWriters);
        try {
            // WriteTask holds a single cancel hook; concurrent shards would otherwise overwrite
            // each other's. Install one hook up front that fans a cancel() out to every shard's
            // statement, collected as each shard starts (see tryWriteShard).
            List<Runnable> shardCancelHooks = new CopyOnWriteArrayList<>();
            if (!writeTask.setCancelHook(() -> shardCancelHooks.forEach(Runnable::run))) {
                throw new IllegalStateException("Write task was cancelled");
            }

            long start = System.nanoTime();
            List<IngestionResult> shardResults = writeAllShards(writeTask, shardCancelHooks);
            long copyDone = System.nanoTime();
            commitAllShards(shardResults);
            long postIngestDone = System.nanoTime();
            accumulatePhaseTimings(copyDone - start, postIngestDone - copyDone);
            logger.debug("Queue '{}' partitioned commit phases: data(COPY)={}ms, postIngest(catalog)={}ms, shards={}",
                    queueId, (copyDone - start) / 1_000_000, (postIngestDone - copyDone) / 1_000_000, parallelWriters);

            IngestionResult merged = mergeResults(shardResults, writeTask);
            writeTask.bucket().futures().forEach(action -> action.complete(merged));
        } catch (Exception e) {
            logger.atError().setCause(e).log("Failed partitioned write to queue {} ({} shards)", queueId, parallelWriters);
            // Same propagation contract as ParquetIngestionQueue.write(): rethrow so
            // BulkIngestQueue.processWriteQueue accounts this bucket as failed and rolls back
            // producer sequences before the futures complete.
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(e);
        } finally {
            cleanupInputFiles(writeTask);
        }
    }

    /**
     * Runs every shard's {@code COPY} concurrently. Nothing is committed to the catalog here — a
     * shard failure at this stage means zero catalog registrations happened for this batch.
     */
    private List<IngestionResult> writeAllShards(WriteTask<String, IngestionResult> writeTask,
                                                  List<Runnable> shardCancelHooks) {
        List<Future<IngestionResult>> futures = new ArrayList<>(parallelWriters);
        for (int i = 0; i < parallelWriters; i++) {
            int shard = i;
            futures.add(shardExecutor.submit(() -> tryWriteShard(writeTask, shard, shardCancelHooks)));
        }
        List<IngestionResult> results = new ArrayList<>(parallelWriters);
        List<String> failures = new ArrayList<>();
        Throwable firstFailure = null;
        for (int i = 0; i < futures.size(); i++) {
            try {
                results.add(futures.get(i).get());
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause() != null ? ee.getCause() : ee;
                failures.add("shard " + i + ": " + cause.getMessage());
                if (firstFailure == null) firstFailure = cause;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                failures.add("shard " + i + ": interrupted");
                if (firstFailure == null) firstFailure = ie;
            }
        }
        if (!failures.isEmpty()) {
            throw new RuntimeException(
                    "Partitioned write failed for queue '%s': %d of %d shard(s) failed before any commit "
                            + "(no partial commit occurred): %s"
                            .formatted(queueId, failures.size(), parallelWriters, String.join("; ", failures)),
                    firstFailure);
        }
        return results;
    }

    private IngestionResult tryWriteShard(WriteTask<String, IngestionResult> writeTask, int shard,
                                          List<Runnable> shardCancelHooks) throws Exception {
        String filter = "hash(%s) %% %d = %d".formatted(HeaderUtils.quoteIdentifier(partitionColumn), parallelWriters, shard);
        String filenamePattern = "dd_shard%d_{uuid}".formatted(shard);
        return tryWrite(writeTask, filter, filenamePattern, shardCancelHooks::add);
    }

    /**
     * Registers every shard with the catalog, in order. If shard {@code k} fails after shards
     * {@code 0..k-1} already committed, those commits cannot be rolled back — logged loudly as a
     * partial-commit anomaly (their data is durable, just not what "no partial commits" promised)
     * and rethrown so the failure is visible rather than swallowed.
     */
    private void commitAllShards(List<IngestionResult> shardResults) {
        for (int i = 0; i < shardResults.size(); i++) {
            try {
                postIngestionHandler.createPostIngestionTask(shardResults.get(i)).execute();
            } catch (Exception e) {
                if (i > 0) {
                    logger.error("Partitioned write for queue '{}': shard {} of {} failed to commit AFTER "
                                    + "shards 0..{} already committed — PARTIAL COMMIT: their data is durable "
                                    + "in the catalog even though this batch is being reported as failed",
                            queueId, i, shardResults.size(), i - 1, e);
                }
                throw e instanceof RuntimeException re ? re : new RuntimeException(e);
            }
        }
    }

    /**
     * One caller-facing {@link IngestionResult} for {@code writeTask}'s futures. The real DuckLake
     * commits already happened per-shard in {@link #commitAllShards}; this is purely informational.
     */
    private IngestionResult mergeResults(List<IngestionResult> shardResults, WriteTask<String, IngestionResult> writeTask) {
        long totalRows = shardResults.stream().mapToLong(IngestionResult::rowCount).sum();
        List<String> allFiles = shardResults.stream()
                .flatMap(r -> r.filesCreated().stream())
                .toList();
        List<List<String>> allWatermarkRows = shardResults.stream()
                .map(IngestionResult::watermarkRows)
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .toList();
        String combinedQuery = shardResults.stream().map(IngestionResult::query).collect(Collectors.joining("\n"));
        return new IngestionResult(this.queueId, writeTask.taskId(), this.applicationId,
                writeTask.bucket().getProducerMaxBatchId(), totalRows, allFiles, combinedQuery,
                allWatermarkRows.isEmpty() ? null : allWatermarkRows);
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
        } finally {
            if (shardExecutor != null) {
                shardExecutor.shutdownNow();
            }
        }
    }
}
