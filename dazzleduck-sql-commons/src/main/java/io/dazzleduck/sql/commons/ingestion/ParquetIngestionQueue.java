package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class ParquetIngestionQueue extends BulkIngestQueue<String, IngestionResult> {

    private final String path;

    /**
     * Thw write will be performed as soon as bucket is full or after the maxDelay is exported since the first batch is inserted
     *
     * @param identifier      identify the queue. Generally this will the path of the bucket
     * @param maxBucketSize   size of the bucket. Write will be performed as soon as bucket is full or overflowing
     * @param maxDelay        write will be performed just after this delay.
     * @param executorService Executor service.
     * @param clock
     */
    public ParquetIngestionQueue(String path, String identifier, long maxBucketSize, Duration maxDelay, ScheduledExecutorService executorService, Clock clock) {
        super(identifier, maxBucketSize, maxDelay, executorService, clock);
        this.path = path;
    }

    @Override
    protected void write(WriteTask<String, IngestionResult> writeTask) {
        var batches = writeTask.bucket().batches();
        // All Arrow files
        var arrowFiles = batches.stream().map(Batch::record).map("'%s'"::formatted).collect(Collectors.joining(","));
        // Last transformation
        var lastTransformation = batches.stream().map(Batch::transformations).filter(Objects::nonNull).flatMap(Arrays::stream).map(String::trim).filter(s -> !s.isEmpty()).distinct().toList().stream().reduce((a, b) -> b).orElse("");
        // Last sort order
        var lastSortOrder = batches.stream().map(Batch::sortOrder).filter(Objects::nonNull).flatMap(Arrays::stream).map(String::trim).filter(s -> !s.isEmpty()).distinct().reduce((a, b) -> b).map(s -> " ORDER BY " + s).orElse("");
        // Last partition
        var lastPartition = batches.stream().map(Batch::partitions).filter(Objects::nonNull).flatMap(Arrays::stream).map(String::trim).filter(s -> !s.isEmpty()).distinct().reduce((a, b) -> b).map(s -> ", PARTITION_BY (" + s + ")").orElse("");
        // Select clause
        var selectClause = lastTransformation.isEmpty() ? "*" : "*, " + lastTransformation;
        // Last format
        var format = batches.isEmpty() ? "" : batches.get(batches.size() - 1).format();
        // Build SQL
        var sql = """
                COPY
                    (SELECT %s FROM read_arrow([%s])%s)
                    TO '%s'
                    (FORMAT %s %s);
                """.formatted(selectClause, arrowFiles, lastSortOrder, this.path, format, lastPartition);
        ConnectionPool.execute(sql);
        writeTask.bucket().futures().forEach(action -> action.complete(new IngestionResult(this.path)));
    }
}
