package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ingestion.BulkIngestQueue;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

public class MockBulkIngestQueue extends BulkIngestQueue<String, MockWriteResult> {
    public MockBulkIngestQueue(String identifier,
                               long minBatchSize,
                               Duration maxDelay,
                               ScheduledExecutorService executorService,
                               Clock clock) {
        super(identifier, minBatchSize, maxDelay, executorService, clock);
    }

    @Override
    protected void write(WriteTask<String, MockWriteResult> writeTask) {
        for (var future : writeTask.bucket().futures()) {
            future.complete(new MockWriteResult(writeTask.taskId(), writeTask.size()));
        }
    }
}
