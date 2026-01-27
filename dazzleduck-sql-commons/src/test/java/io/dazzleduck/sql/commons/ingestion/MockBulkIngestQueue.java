package io.dazzleduck.sql.commons.ingestion;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

public class MockBulkIngestQueue extends BulkIngestQueue<String, MockWriteResult> {
    public MockBulkIngestQueue(String identifier,
                               long minBatchSize,
                               int maxBatches,
                               long maxPendingWrite,
                               Duration maxDelay,
                               ScheduledExecutorService executorService,
                               Clock clock) {
        super(identifier, minBatchSize, maxBatches, maxPendingWrite, maxDelay, executorService, clock);
    }

    @Override
    public void write(WriteTask<String, MockWriteResult> writeTask) {
        for (var future : writeTask.bucket().futures()) {
            future.complete(new MockWriteResult(writeTask.taskId(), writeTask.size()));
        }
    }
}
