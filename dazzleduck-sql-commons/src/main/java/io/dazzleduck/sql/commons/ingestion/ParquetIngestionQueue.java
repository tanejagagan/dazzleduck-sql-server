package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class ParquetIngestionQueue extends BulkIngestQueue<String, IngestionResult> {

    private static final Logger logger = LoggerFactory.getLogger(ParquetIngestionQueue.class);

    /**
     * Virtual thread executor for async file cleanup.
     * Virtual threads are ideal for I/O-bound tasks like file deletion.
     * This is a shared executor - virtual threads are lightweight so no pooling needed.
     */
    private static final ExecutorService CLEANUP_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    protected final String outputPath;
    protected final String queueId;
    protected final IngestionHandler postIngestionHandler;
    protected final String applicationId;
    protected final String inputFormat;
    protected final String parquetCompression;

    /**
     * Per-phase commit timings. The write is two phases with different parallelism potential:
     * the data phase (DuckDB COPY to Parquet — internally multi-threaded, and overlappable
     * across commit lanes) and the post-ingestion phase (e.g. the DuckLake catalog commit —
     * single-writer, serialized no matter how many lanes exist). Their ratio bounds what
     * parallelizing the commit path can gain (Amdahl: max speedup = (data + post) / post),
     * so both are tracked separately and exposed as metrics.
     */
    private final java.util.concurrent.atomic.LongAccumulator dataPhaseNanos =
            new java.util.concurrent.atomic.LongAccumulator(Long::sum, 0L);
    private final java.util.concurrent.atomic.LongAccumulator postIngestPhaseNanos =
            new java.util.concurrent.atomic.LongAccumulator(Long::sum, 0L);

    /**
     * @param applicationId    producer identifier
     * @param inputFormat      source file format (e.g. {@code "parquet"}, {@code "arrow"})
     * @param outputPath       destination path for written Parquet files
     * @param ingestionQueue   queue identifier — passed to {@link IngestionHandler#getTransformation}
     *                         on every write so the transformation is always current
     * @param minBucketSize    flush when accumulated size reaches this threshold (bytes)
     * @param maxBucketSize    hard upper limit before forced flush (bytes)
     * @param maxBatches       max number of batches before forced flush
     * @param maxPendingWrite  backpressure limit (bytes)
     * @param maxDelay         time-based flush interval
     * @param postIngestionHandler handler that provides transformation SQL and post-write tasks
     * @param executorService  scheduler for time-based flush
     * @param clock            clock for scheduling
     */
    public ParquetIngestionQueue(String applicationId,
                                 String inputFormat,
                                 String outputPath,
                                 String ingestionQueue,
                                 long minBucketSize,
                                 long maxBucketSize,
                                 int maxBatches,
                                 long maxPendingWrite,
                                 Duration maxDelay,
                                 IngestionHandler postIngestionHandler,
                                 ScheduledExecutorService executorService,
                                 Clock clock) {
        this(applicationId, inputFormat, outputPath, ingestionQueue, minBucketSize, maxBucketSize,
                maxBatches, maxPendingWrite, maxDelay, null, postIngestionHandler, executorService, clock);
    }

    /**
     * @param parquetCompression codec for written Parquet files, or {@code null} for DuckDB's
     *                           default; see {@link IngestionConfig#parquetCompression()}
     */
    public ParquetIngestionQueue(String applicationId,
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
                                 Clock clock) {
        super(ingestionQueue, minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, executorService, clock);
        this.outputPath = outputPath;
        this.queueId = ingestionQueue;
        this.postIngestionHandler = postIngestionHandler;
        this.applicationId = applicationId;
        this.inputFormat = inputFormat;
        this.parquetCompression = parquetCompression;
        // The output path (local or object store) is expected to already exist — provisioning it is
        // the operator's responsibility, outside the scope of this project. We never create it here.
    }

    @Override
    public void write(WriteTask<String, IngestionResult> writeTask) {
        logger.debug("Ingestion queue '{}' received batch with {} files, outputPath={}",
                queueId, writeTask.bucket().batches().size(), outputPath);
        try {
            long start = System.nanoTime();
            IngestionResult ingestionResult = tryWrite(writeTask);
            long copyDone = System.nanoTime();
            var postIngestionTask = postIngestionHandler.createPostIngestionTask(ingestionResult);
            postIngestionTask.execute();
            long postIngestDone = System.nanoTime();
            dataPhaseNanos.accumulate(copyDone - start);
            postIngestPhaseNanos.accumulate(postIngestDone - copyDone);
            logger.debug("Queue '{}' commit phases: data(COPY)={}ms, postIngest(catalog)={}ms",
                    queueId, (copyDone - start) / 1_000_000, (postIngestDone - copyDone) / 1_000_000);
            writeTask.bucket().futures().forEach(action -> action.complete(ingestionResult));
        } catch (Exception e) {
            var sql = constructWriteQuery(writeTask);
            logger.atError().setCause(e).log("Failed to write to queue {} sql {}", queueId, sql);
            // Propagate instead of completing the futures here: BulkIngestQueue.processWriteQueue
            // must account this bucket as failed (pendingWrite stays truthful, failed-write
            // metrics accumulate) and roll back producer sequences BEFORE the futures complete,
            // so a client observing the failure can immediately retry the same batch. Swallowing
            // the exception would make the failed bytes count as written and leave retries
            // rejected as OutOfSequenceBatch.
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(e);
        } finally {
            cleanupInputFiles(writeTask);
        }
    }

    /** Cumulative nanoseconds spent in the data phase (DuckDB COPY to Parquet). */
    public long getDataPhaseNanos() {
        return dataPhaseNanos.get();
    }

    /** Cumulative nanoseconds spent in the post-ingestion phase (e.g. DuckLake catalog commit). */
    public long getPostIngestPhaseNanos() {
        return postIngestPhaseNanos.get();
    }

    /** Feeds the same per-phase timing accumulators {@link #write} updates, for subclasses that
     * override {@code write} with a different commit shape (e.g. multiple parallel shards). */
    protected void accumulatePhaseTimings(long dataPhaseElapsedNanos, long postIngestPhaseElapsedNanos) {
        dataPhaseNanos.accumulate(dataPhaseElapsedNanos);
        postIngestPhaseNanos.accumulate(postIngestPhaseElapsedNanos);
    }

    /**
     * Asynchronously cleans up input files using virtual threads.
     * This is fire-and-forget - we don't wait for deletion to complete
     * since it doesn't affect the write result.
     */
    protected void cleanupInputFiles(WriteTask<String, IngestionResult> writeTask) {
        writeTask.bucket().batches().forEach(this::onBatchAbandoned);
    }

    @Override
    protected void onBatchAbandoned(Batch<String> batch) {
        final String filePath = batch.record();
        CLEANUP_EXECUTOR.execute(() -> {
            try {
                Files.deleteIfExists(Path.of(filePath));
            } catch (Exception e) {
                logger.warn("Failed to delete temporary input file: {}", filePath, e);
            }
        });
    }

    protected String getClause(String[] values, String clause){
        if(values == null || values.length == 0){
            return "";
        } else {
            var nested = Arrays.stream(values).filter(Objects::nonNull).map(String::trim).collect(Collectors.joining(","));
            return clause.formatted(nested);
        }
    }

    /**
     * The relation the output files are written from: the temp input files with the
     * transformation and any partition projections applied. This is also the relation the
     * watermark rows are computed over — same schema, same rows as the written output, read
     * while the input files are still local.
     */
    protected String constructSourceRelation(WriteTask<String, IngestionResult> writeTask) {
        return constructSourceRelation(writeTask, null);
    }

    /**
     * @param additionalFilter extra SQL boolean expression the relation is filtered by (e.g. a
     *                         shard's {@code hash(col) % N = i} routing predicate), or {@code null}
     *                         for the whole relation
     */
    protected String constructSourceRelation(WriteTask<String, IngestionResult> writeTask, String additionalFilter) {
        return constructSourceRelation(writeTask.bucket().batches(), additionalFilter);
    }

    /**
     * @param batches the specific batches to read — normally the whole flushed bucket, but a
     *                subclass verifying which individual batch(es) match a condition (e.g.
     *                {@link PartitionedIngestionQueue}'s failure attribution) can pass just one
     */
    protected String constructSourceRelation(List<Batch<String>> batches, String additionalFilter) {
        // All Arrow files
        var arrowFiles = batches.stream().map(Batch::record).map("'%s'"::formatted).collect(Collectors.joining(","));
        String[] batchPartitionBy = batches.get(0).partitionBy();
        boolean hasBatchPartitionBy = batchPartitionBy != null && batchPartitionBy.length > 0;
        String sortOrderClause = getClause(batches.get(0).sortOrder(), "ORDER BY %s ");

        // Inner SQL reads from the temp Arrow files
        var innerSql = "SELECT * FROM read_%s([%s]) %s".formatted(this.inputFormat, arrowFiles, sortOrderClause);

        // Fetch transformation fresh from the handler on every write so view-based
        // and handler-refreshed transformations are always current without caching.
        String transformation = postIngestionHandler.getTransformation(queueId);
        var querySql = (transformation != null && !transformation.isBlank())
                ? "WITH __this AS (%s) %s".formatted(innerSql, transformation)
                : innerSql;

        // Add handler-supplied derived-column projections (e.g. day(timestamp) AS day) so the
        // PARTITION_BY tokens resolve. Not needed for header-supplied partitionBy, which names
        // columns that already exist in the relation.
        if (!hasBatchPartitionBy) {
            String[] partitionProjections = postIngestionHandler.getPartitionProjections(queueId);
            if (partitionProjections.length > 0) {
                querySql = "SELECT *, %s FROM (%s)".formatted(
                        String.join(", ", partitionProjections), querySql);
            }
        }
        if (additionalFilter != null) {
            querySql = "SELECT * FROM (%s) WHERE %s".formatted(querySql, additionalFilter);
        }
        return querySql;
    }

    protected String constructWriteQuery(WriteTask<String, IngestionResult> writeTask) {
        return constructWriteQuery(writeTask, null, null);
    }

    /**
     * @param additionalFilter extra filter applied to the source relation, or {@code null}
     * @param filenamePattern  {@code FILENAME_PATTERN} value for the COPY statement, or {@code null}
     *                         to omit the clause (the unpartitioned branch below already generates
     *                         a unique file name per call). Required whenever more than one COPY can
     *                         target the same output directory concurrently (see
     *                         {@link PartitionedIngestionQueue}), since DuckDB's default per-COPY
     *                         file counter can otherwise collide across concurrent invocations.
     */
    protected String constructWriteQuery(WriteTask<String, IngestionResult> writeTask, String additionalFilter, String filenamePattern) {
        var batches = writeTask.bucket().batches();
        String[] batchPartitionBy = batches.get(0).partitionBy();
        boolean hasBatchPartitionBy = batchPartitionBy != null && batchPartitionBy.length > 0;
        String[] effectivePartitionBy = hasBatchPartitionBy
                ? batchPartitionBy
                : postIngestionHandler.getPartitionBy(queueId);
        String partitionByClause = getClause(effectivePartitionBy, ", PARTITION_BY(%s)");
        // Last format
        var outputFormat = batches.isEmpty() ? "" : batches.get(batches.size() - 1).format();
        String fullFilePath;
        // FILENAME_PATTERN only applies to COPY's directory form (PARTITION_BY set below); the
        // unpartitioned form writes to one explicit file path, so a caller-supplied pattern (e.g. a
        // shard prefix) is honored here directly, keeping file naming consistent either way.
        if (partitionByClause.isEmpty()) {
            String uniqueFileName = (filenamePattern != null
                    ? filenamePattern.replace("{uuid}", UUID.randomUUID().toString())
                    : "dd_" + UUID.randomUUID()) + "." + outputFormat;
            fullFilePath = this.outputPath + "/" + uniqueFileName;
        } else {
            fullFilePath = this.outputPath;
        }

        var querySql = constructSourceRelation(writeTask, additionalFilter);

        String compressionClause = parquetCompression != null && "parquet".equalsIgnoreCase(outputFormat)
                ? ", COMPRESSION %s".formatted(parquetCompression) : "";
        String filenamePatternClause = filenamePattern != null && !partitionByClause.isEmpty()
                ? ", FILENAME_PATTERN '%s'".formatted(filenamePattern) : "";

        // Build SQL
        // https://duckdb.org/docs/stable/sql/statements/copy
        var sql = """
                COPY
                    (%s)
                    TO '%s'
                    (FORMAT %s%s %s, RETURN_FILES, APPEND%s);
                """.formatted(querySql, fullFilePath, outputFormat, compressionClause, partitionByClause, filenamePatternClause);
        return sql;
    }

    protected IngestionResult tryWrite(WriteTask<String, IngestionResult> writeTask) throws Exception {
        return tryWrite(writeTask, null, null);
    }

    protected IngestionResult tryWrite(WriteTask<String, IngestionResult> writeTask, String additionalFilter, String filenamePattern) throws Exception {
        return tryWrite(writeTask, additionalFilter, filenamePattern, writeTask::setCancelHook);
    }

    /**
     * @param cancelHookInstaller registers this call's cancel action, returning {@code false} (and
     *                            aborting before the COPY runs) if the task was already cancelled —
     *                            same contract as {@link WriteTask#setCancelHook}, which is exactly
     *                            what the single-shard overload above passes. A subclass running
     *                            several shards concurrently over the same {@link WriteTask} passes
     *                            an installer that fans a single external {@code cancel()} call out
     *                            to every shard's statement, since {@code WriteTask} only holds one
     *                            hook and concurrent shards would otherwise overwrite each other's.
     */
    protected IngestionResult tryWrite(WriteTask<String, IngestionResult> writeTask, String additionalFilter,
                                        String filenamePattern, java.util.function.Predicate<Runnable> cancelHookInstaller) throws Exception {
        var sql = constructWriteQuery(writeTask, additionalFilter, filenamePattern);
        logger.debug("Executing COPY SQL: {}", sql);
        List<String> files = new ArrayList<>();
        long count = 0;
        // Watermark rows are computed BEFORE the COPY, over the same source relation the output
        // is written from: the data is still local, the transformation is already applied (so
        // partition columns are real typed columns, not hive path fragments), and a misconfigured
        // spec fails fast without leaving an unregistered output file behind.
        WatermarkSpec watermarkSpec = postIngestionHandler.getWatermarkSpec(queueId);
        List<List<String>> watermarkRows = null;
        try (var conn = ConnectionPool.getConnection();
             var stmt = conn.createStatement()) {

            if (watermarkSpec != null) {
                watermarkRows = watermarkSpec.computeRows(conn, constructSourceRelation(writeTask, additionalFilter));
            }

            // Set up cancellation hook
            var cancelHookSet = cancelHookInstaller.test(() -> {
                try {
                    stmt.cancel();
                } catch (Exception e) {
                    // Ignore cancellation errors
                }
            });

            // If cancel was already called, don't execute the query
            if (!cancelHookSet) {
                throw new IllegalStateException("Write task was cancelled");
            }

            // Execute the query using our statement so the cancel hook works
            stmt.execute(sql);
            try (var rs = stmt.getResultSet()) {
                while (rs.next()) {
                    var rowCount = rs.getLong("count");
                    var rowFilesArray = rs.getArray("files");
                    count += rowCount;
                    if (rowFilesArray != null) {
                        var rowFiles = (Object[]) rowFilesArray.getArray();
                        files.addAll(Arrays.stream(rowFiles).map(Object::toString).toList());
                    }
                }
            }
        }
        logger.debug("COPY completed for queue '{}': {} rows written, {} files: {}",
                queueId, count, files.size(), files);
        return new IngestionResult(this.queueId, writeTask.taskId(), this.applicationId,
                writeTask.bucket().getProducerMaxBatchId(),
                count,
                files, sql, watermarkRows);
    }
}
