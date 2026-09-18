package io.dazzleduck.sql.commons.ingestion;

import java.util.List;

/**
 * Point-in-time per-queue ingestion statistics, rendered by the stats dashboards.
 *
 * <p>Most fields are cumulative counters ({@code totalWrite*}, {@code failed*}, {@code rejected*},
 * {@code rowsWritten}, {@code producerIdEvictions}); {@code pending*} are current gauges;
 * {@code last*EpochMs} are wall-clock instants (0 = never). For a
 * {@link PartitionedIngestionQueue} the top-level values are aggregated across its children and
 * {@link #partitions()} holds one entry per child (identifier {@code p0..pN-1}); for an ordinary
 * queue {@link #partitions()} is empty.
 *
 * @param rejected429            batches rejected for backpressure ({@link PendingWriteExceededException})
 * @param rejectedOutOfSequence  batches rejected as out-of-sequence duplicates ({@link OutOfSequenceBatch})
 * @param rejectedMultiPartition batches rejected because their rows spanned more than one partition
 *                               (partitioned queues only)
 * @param pendingBytes           bytes accepted but not yet written (current)
 * @param maxPendingWrite        backpressure limit in bytes (for a "% full" reading)
 * @param dataPhaseMillis        cumulative ms in the COPY-to-Parquet phase
 * @param postIngestMillis       cumulative ms in the post-ingestion (e.g. catalog commit) phase
 * @param lastWriteEpochMs       instant the most recent bucket finished writing (0 = never)
 * @param lastReceiveEpochMs     instant the most recent batch was accepted (0 = never)
 * @param lastErrorEpochMs       instant of the most recent write failure (0 = never)
 * @param lastError              message of the most recent write failure, or null
 * @param partitions             per-partition child stats (empty unless this is a partitioned queue)
 */
public record Stats(String identifier,
                    long totalWriteBytes,
                    long totalWriteBatches,
                    long totalWriteBuckets,
                    long timeSpentWriting,
                    long pendingBatches,
                    long pendingBuckets,
                    long failedWriteBytes,
                    long failedWriteBatches,
                    long failedWriteBuckets,
                    long producerIdEvictions,
                    long rowsWritten,
                    long pendingBytes,
                    long maxPendingWrite,
                    long dataPhaseMillis,
                    long postIngestMillis,
                    long rejected429,
                    long rejectedOutOfSequence,
                    long rejectedMultiPartition,
                    long lastWriteEpochMs,
                    long lastReceiveEpochMs,
                    long lastErrorEpochMs,
                    String lastError,
                    List<Stats> partitions) {

    public Stats {
        partitions = partitions == null ? List.of() : List.copyOf(partitions);
    }

    /** Returns a copy with a different {@code identifier} (used to label partition child rows). */
    public Stats withIdentifier(String newIdentifier) {
        return new Stats(newIdentifier, totalWriteBytes, totalWriteBatches, totalWriteBuckets,
                timeSpentWriting, pendingBatches, pendingBuckets, failedWriteBytes, failedWriteBatches,
                failedWriteBuckets, producerIdEvictions, rowsWritten, pendingBytes, maxPendingWrite,
                dataPhaseMillis, postIngestMillis, rejected429, rejectedOutOfSequence, rejectedMultiPartition,
                lastWriteEpochMs, lastReceiveEpochMs, lastErrorEpochMs, lastError, partitions);
    }

    public static Builder builder(String identifier) {
        return new Builder(identifier);
    }

    /** Fluent builder so the (large) call sites in the queue classes stay readable. */
    public static final class Builder {
        private final String identifier;
        private long totalWriteBytes, totalWriteBatches, totalWriteBuckets, timeSpentWriting;
        private long pendingBatches, pendingBuckets, pendingBytes, maxPendingWrite;
        private long failedWriteBytes, failedWriteBatches, failedWriteBuckets, producerIdEvictions;
        private long rowsWritten, dataPhaseMillis, postIngestMillis;
        private long rejected429, rejectedOutOfSequence, rejectedMultiPartition;
        private long lastWriteEpochMs, lastReceiveEpochMs, lastErrorEpochMs;
        private String lastError;
        private List<Stats> partitions = List.of();

        private Builder(String identifier) { this.identifier = identifier; }

        public Builder totalWriteBytes(long v)      { this.totalWriteBytes = v; return this; }
        public Builder totalWriteBatches(long v)    { this.totalWriteBatches = v; return this; }
        public Builder totalWriteBuckets(long v)    { this.totalWriteBuckets = v; return this; }
        public Builder timeSpentWriting(long v)     { this.timeSpentWriting = v; return this; }
        public Builder pendingBatches(long v)       { this.pendingBatches = v; return this; }
        public Builder pendingBuckets(long v)       { this.pendingBuckets = v; return this; }
        public Builder pendingBytes(long v)         { this.pendingBytes = v; return this; }
        public Builder maxPendingWrite(long v)      { this.maxPendingWrite = v; return this; }
        public Builder failedWriteBytes(long v)     { this.failedWriteBytes = v; return this; }
        public Builder failedWriteBatches(long v)   { this.failedWriteBatches = v; return this; }
        public Builder failedWriteBuckets(long v)   { this.failedWriteBuckets = v; return this; }
        public Builder producerIdEvictions(long v)  { this.producerIdEvictions = v; return this; }
        public Builder rowsWritten(long v)          { this.rowsWritten = v; return this; }
        public Builder dataPhaseMillis(long v)      { this.dataPhaseMillis = v; return this; }
        public Builder postIngestMillis(long v)     { this.postIngestMillis = v; return this; }
        public Builder rejected429(long v)          { this.rejected429 = v; return this; }
        public Builder rejectedOutOfSequence(long v){ this.rejectedOutOfSequence = v; return this; }
        public Builder rejectedMultiPartition(long v){ this.rejectedMultiPartition = v; return this; }
        public Builder lastWriteEpochMs(long v)     { this.lastWriteEpochMs = v; return this; }
        public Builder lastReceiveEpochMs(long v)   { this.lastReceiveEpochMs = v; return this; }
        public Builder lastErrorEpochMs(long v)     { this.lastErrorEpochMs = v; return this; }
        public Builder lastError(String v)          { this.lastError = v; return this; }
        public Builder partitions(List<Stats> v)    { this.partitions = v == null ? List.of() : v; return this; }

        public Stats build() {
            return new Stats(identifier, totalWriteBytes, totalWriteBatches, totalWriteBuckets,
                    timeSpentWriting, pendingBatches, pendingBuckets, failedWriteBytes, failedWriteBatches,
                    failedWriteBuckets, producerIdEvictions, rowsWritten, pendingBytes, maxPendingWrite,
                    dataPhaseMillis, postIngestMillis, rejected429, rejectedOutOfSequence, rejectedMultiPartition,
                    lastWriteEpochMs, lastReceiveEpochMs, lastErrorEpochMs, lastError, partitions);
        }
    }
}
