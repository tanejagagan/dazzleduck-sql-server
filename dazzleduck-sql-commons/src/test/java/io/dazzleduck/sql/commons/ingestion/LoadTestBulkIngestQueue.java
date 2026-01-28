package io.dazzleduck.sql.commons.ingestion;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.LongAdder;

/**
 * A mock BulkIngestQueue for load testing with configurable write latency.
 */
public class LoadTestBulkIngestQueue extends BulkIngestQueue<String, MockWriteResult> {
    private final Duration writeLatency;
    private final LongAdder writeCount = new LongAdder();
    private final LongAdder combinedBucketCount = new LongAdder();

    public LoadTestBulkIngestQueue(String identifier,
                                   long minBucketSize,
                                   long maxBucketSize,
                                   int maxBatches,
                                   long maxPendingWrite,
                                   Duration maxDelay,
                                   Duration writeLatency,
                                   ScheduledExecutorService executorService,
                                   Clock clock) {
        super(identifier, minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, executorService, clock);
        this.writeLatency = writeLatency;
    }

    @Override
    public void write(WriteTask<String, MockWriteResult> writeTask) {
        // Simulate write latency
        if (!writeLatency.isZero()) {
            try {
                Thread.sleep(writeLatency.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Write interrupted", e);
            }
        }

        writeCount.increment();

        for (var future : writeTask.bucket().futures()) {
            future.complete(new MockWriteResult(writeTask.taskId(), writeTask.size()));
        }
    }

    public long getWriteCount() {
        return writeCount.sum();
    }
}
