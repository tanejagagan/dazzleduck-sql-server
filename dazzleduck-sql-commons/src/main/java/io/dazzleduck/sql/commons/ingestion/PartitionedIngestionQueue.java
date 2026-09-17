package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.HeaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
 * before any shard's post-ingestion task runs — {@link #writeAllShards} does the writes first, and
 * only shards that succeeded are then registered with the catalog. A failure during that second
 * phase, after some shards already committed, is a genuine residual risk this design cannot remove
 * — it is logged loudly as a partial-commit anomaly rather than hidden.
 *
 * <p><b>Failure attribution.</b> When some shards fail and others succeed, {@link #write} does not
 * simply fail the whole flushed bucket: {@link #attributeAffectedFiles} re-evaluates just the failed
 * shards' routing filters against the raw (pre-transformation) input, tagged with each row's source
 * file, to determine exactly which input batches had rows routed to a failed shard. Batches with no
 * rows in any failed shard have their futures completed normally; only the affected batches' futures
 * fail, via {@link PartialWriteFailure} so {@link BulkIngestQueue} accounts for and rolls back
 * producer sequences for just that subset. Attribution can itself be unavailable (e.g. the
 * partition column doesn't exist pre-transformation, every shard failed, or the same broken
 * expression that failed a shard also breaks this query) — in that case every batch in the flush is
 * conservatively treated as affected, i.e. the whole-bucket failure behavior from before this
 * attribution existed.
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

    /** Every shard's outcome from {@link #writeAllShards}, keyed by shard index in submission order. */
    private record ShardOutcome(Map<Integer, IngestionResult> succeeded, Map<Integer, Throwable> failed) {}

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
            ShardOutcome copyOutcome = writeAllShards(writeTask, shardCancelHooks);
            long copyDone = System.nanoTime();

            // Commit only the shards whose COPY succeeded; a commit failure joins failedShards too,
            // scoped to that one shard exactly like a COPY failure would be.
            List<IngestionResult> committedResults = new ArrayList<>();
            Map<Integer, Throwable> failedShards = new LinkedHashMap<>(copyOutcome.failed());
            for (var entry : copyOutcome.succeeded().entrySet()) {
                int shard = entry.getKey();
                try {
                    postIngestionHandler.createPostIngestionTask(entry.getValue()).execute();
                    committedResults.add(entry.getValue());
                } catch (Exception e) {
                    if (!committedResults.isEmpty()) {
                        logger.error("Partitioned write for queue '{}': shard {} [{}] failed to commit AFTER {} "
                                        + "other shard(s) already committed — PARTIAL COMMIT: their data is durable "
                                        + "in the catalog regardless of how this batch is ultimately reported",
                                queueId, shard, shardFilter(shard), committedResults.size(), e);
                    }
                    failedShards.put(shard, e);
                }
            }
            long postIngestDone = System.nanoTime();
            accumulatePhaseTimings(copyDone - start, postIngestDone - copyDone);
            logger.debug("Queue '{}' partitioned commit phases: data(COPY)={}ms, postIngest(catalog)={}ms, shards={}",
                    queueId, (copyDone - start) / 1_000_000, (postIngestDone - copyDone) / 1_000_000, parallelWriters);

            if (failedShards.isEmpty()) {
                IngestionResult merged = mergeResults(committedResults, writeTask);
                writeTask.bucket().futures().forEach(action -> action.complete(merged));
                return;
            }
            handleShardFailures(writeTask, committedResults, failedShards);
        } catch (Exception e) {
            logger.atError().setCause(e).log("Failed partitioned write to queue {} ({} shards)", queueId, parallelWriters);
            // Same propagation contract as ParquetIngestionQueue.write(): rethrow so
            // BulkIngestQueue.processWriteQueue accounts this bucket as failed (in full, or scoped
            // to PartialWriteFailure.failedBatches() when attribution found a genuine split) and
            // rolls back the corresponding producer sequences.
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
     * shard failure at this stage means zero catalog registrations happened for that shard's data.
     */
    private ShardOutcome writeAllShards(WriteTask<String, IngestionResult> writeTask, List<Runnable> shardCancelHooks) {
        List<Future<IngestionResult>> futures = new ArrayList<>(parallelWriters);
        for (int i = 0; i < parallelWriters; i++) {
            int shard = i;
            futures.add(shardExecutor.submit(() -> tryWriteShard(writeTask, shard, shardCancelHooks)));
        }
        Map<Integer, IngestionResult> succeeded = new LinkedHashMap<>();
        Map<Integer, Throwable> failed = new LinkedHashMap<>();
        for (int i = 0; i < futures.size(); i++) {
            try {
                succeeded.put(i, futures.get(i).get());
            } catch (ExecutionException ee) {
                failed.put(i, ee.getCause() != null ? ee.getCause() : ee);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                failed.put(i, ie);
            }
        }
        return new ShardOutcome(succeeded, failed);
    }

    private IngestionResult tryWriteShard(WriteTask<String, IngestionResult> writeTask, int shard,
                                          List<Runnable> shardCancelHooks) throws Exception {
        String filenamePattern = "dd_shard%d_{uuid}".formatted(shard);
        return tryWrite(writeTask, shardFilter(shard), filenamePattern, shardCancelHooks::add);
    }

    /** The row-routing filter for {@code shard}, shared between the actual write and failure messages. */
    private String shardFilter(int shard) {
        return "hash(%s) %% %d = %d".formatted(HeaderUtils.quoteIdentifier(partitionColumn), parallelWriters, shard);
    }

    /**
     * Handles a non-empty {@code failedShards}: tries to attribute the failure(s) to specific input
     * batches so unaffected ones can still succeed, then completes every future in the bucket and
     * throws to signal the outcome to {@link BulkIngestQueue}.
     *
     * <p>Throws a plain {@link RuntimeException} (whole-bucket failure, the pre-attribution behavior)
     * when every batch turns out to be affected — either because every shard failed, or because
     * attribution itself was unavailable. Throws {@link PartialWriteFailure} only for a genuine
     * split, after completing every future in {@code writeTask}'s bucket itself.
     */
    private void handleShardFailures(WriteTask<String, IngestionResult> writeTask,
                                     List<IngestionResult> committedResults,
                                     Map<Integer, Throwable> failedShards) {
        String failureSummary = failedShards.entrySet().stream()
                .map(e -> "shard %d [%s]: %s".formatted(e.getKey(), shardFilter(e.getKey()), e.getValue().getMessage()))
                .collect(Collectors.joining("; "));

        boolean allShardsFailed = failedShards.size() == parallelWriters;
        Set<String> affectedFiles = allShardsFailed ? null : attributeAffectedFiles(writeTask, failedShards.keySet());
        boolean attributionAvailable = affectedFiles != null;

        List<Batch<String>> allBatches = writeTask.bucket().batches();
        List<Integer> failedIndices = new ArrayList<>();
        List<Integer> successfulIndices = new ArrayList<>();
        for (int i = 0; i < allBatches.size(); i++) {
            boolean affected = !attributionAvailable || affectedFiles.contains(allBatches.get(i).record());
            (affected ? failedIndices : successfulIndices).add(i);
        }

        if (successfulIndices.isEmpty()) {
            throw new RuntimeException(
                    ("Partitioned write failed for queue '%s': %d of %d shard(s) failed before any commit "
                            + "(no partial commit occurred)%s: %s")
                            .formatted(queueId, failedShards.size(), parallelWriters,
                                    attributionAvailable ? "" : ", attribution unavailable so every batch in this flush is affected",
                                    failureSummary),
                    failedShards.values().iterator().next());
        }

        // Genuine split: unaffected batches succeed with the committed shards' data; affected
        // batches fail with an error naming exactly which shard(s)/filter(s) they were routed to.
        List<CompletableFuture<IngestionResult>> allFutures = writeTask.bucket().futures();
        IngestionResult merged = mergeResults(committedResults, writeTask);
        for (int idx : successfulIndices) {
            allFutures.get(idx).complete(merged);
        }
        RuntimeException perBatchError = new RuntimeException(
                "Partitioned write for queue '%s': this batch's rows were routed to a failed shard: %s"
                        .formatted(queueId, failureSummary));
        for (int idx : failedIndices) {
            allFutures.get(idx).completeExceptionally(perBatchError);
        }

        logger.error("Partitioned write for queue '{}': {} of {} batches affected by {} failed shard(s) "
                        + "(rest committed successfully): {}",
                queueId, failedIndices.size(), allBatches.size(), failedShards.size(), failureSummary);

        throw new PartialWriteFailure(
                "Partitioned write for queue '%s': %d of %d batches failed (%s)"
                        .formatted(queueId, failedIndices.size(), allBatches.size(), failureSummary),
                failedShards.values().iterator().next(),
                failedIndices.stream().<Batch<?>>map(allBatches::get).toList());
    }

    /**
     * Determines which of this flush's input files (batches) contributed at least one row to any of
     * {@code failedShardIndices}, by re-evaluating just those shards' routing filters against the
     * raw (pre-transformation) input relation tagged with its source file via {@code filename=true}
     * — the same {@code read_%s(...)} function {@link ParquetIngestionQueue#constructSourceRelation}
     * uses, so filenames match {@link Batch#record()} exactly.
     *
     * <p>Deliberately bypasses the configured transformation: {@code partitionColumn} must be a raw
     * input column for this to work, since a transformation can project it away, derive it, or (in
     * principle) change row cardinality, none of which this simple re-evaluation can account for.
     * A queue routing on a transformation-derived column will see every failure attributed to every
     * batch (via the {@code null} return below), i.e. the same whole-bucket behavior as before this
     * attribution existed — not a regression, just not narrowed for that configuration.
     *
     * @return the affected file paths, or {@code null} if the query itself failed (attribution
     *         unavailable — caller falls back to treating every batch as affected)
     */
    private Set<String> attributeAffectedFiles(WriteTask<String, IngestionResult> writeTask, Set<Integer> failedShardIndices) {
        var batches = writeTask.bucket().batches();
        var files = batches.stream().map(Batch::record).map("'%s'"::formatted).collect(Collectors.joining(","));
        String combinedFilter = failedShardIndices.stream().map(this::shardFilter).collect(Collectors.joining(" OR "));
        String query = "SELECT DISTINCT filename FROM read_%s([%s], filename=true) WHERE %s"
                .formatted(inputFormat, files, combinedFilter);
        try (var conn = ConnectionPool.getConnection();
             var stmt = conn.createStatement();
             var rs = stmt.executeQuery(query)) {
            Set<String> affected = new HashSet<>();
            while (rs.next()) {
                affected.add(rs.getString("filename"));
            }
            return affected;
        } catch (Exception e) {
            logger.warn("Queue '{}': could not attribute the failed shard(s) to specific batches ({}); "
                    + "every batch in this flush will be treated as affected", queueId, e.getMessage());
            return null;
        }
    }

    /**
     * One caller-facing {@link IngestionResult}. The real DuckLake commits already happened
     * per-shard in {@link #write}; this is purely informational.
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
