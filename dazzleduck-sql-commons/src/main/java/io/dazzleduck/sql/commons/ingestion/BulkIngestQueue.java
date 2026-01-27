package io.dazzleduck.sql.commons.ingestion;

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAccumulator;

public abstract class BulkIngestQueue<T, R> implements BulkIngestQueueInterface<T, R> {



    public static Path writeAndValidateTempArrowFile(Path tempDir, ArrowReader reader) throws IOException {
        String uniqueFileName = "ingestion_" + UUID.randomUUID() + ".arrow";
        Path tempFilePath = tempDir.resolve(uniqueFileName);
        try (FileOutputStream fos = new FileOutputStream(String.valueOf(tempFilePath));
             ArrowStreamWriter writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, Channels.newChannel(fos))) {
            while (reader.loadNextBatch()){
                writer.writeBatch();
            }
            writer.end();
        }
        return tempFilePath;
    }

    /**
     * Creates a new combined bucket from multiple buckets.
     * The combined bucket contains all batches and futures from the source buckets.
     *
     * @param buckets the list of buckets to combine
     * @param minBucketSize minimum bucket size for the new bucket
     * @param maxBatches maximum batches for the new bucket
     * @param maxDelay maximum delay for the new bucket
     * @return a new bucket containing all content from the source buckets
     */
    static <T, R> Bucket<T, R> combineBuckets(List<Bucket<T, R>> buckets, long minBucketSize, int maxBatches, Duration maxDelay) {
        var combined = new Bucket<T, R>(minBucketSize, maxBatches, maxDelay);
        for (var bucket : buckets) {
            for (int i = 0; i < bucket.batches().size(); i++) {
                combined.add(bucket.batches().get(i), bucket.futures().get(i));
            }
        }
        combined.markFinalized();
        return combined;
    }

    /**
     * Checks if a bucket can be added to a combined bucket without exceeding limits.
     */
    private static <T, R> boolean canCombine(long currentSize, int currentBatchCount,
                                              Bucket<T, R> candidate, long maxSize, int maxBatchCount) {
        return (currentSize + candidate.size() <= maxSize) &&
               (currentBatchCount + candidate.batchCount() <= maxBatchCount);
    }
    private static final int MAX_PRODUCER_IDS = 10000;
    private final Map<String, Long> inProgressBatchIds = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > MAX_PRODUCER_IDS;
        }
    };
    private final long minBucketSize;
    private final long maxBucketSize;
    private final int maxBatches;
    private final long maxPendingWrite;
    private final String identifier;
    private final ScheduledExecutorService executorService;
    private final Duration maxDelay;
    private final Clock clock;
    private Instant lastWrite = Instant.EPOCH;
    private Bucket<T, R> currentBucket;
    private volatile boolean terminating;

    private volatile WriteTask<T, R> runningWrite;
    private long writeTaskId;

    private final BlockingQueue<WriteTask<T, R>> writeQueue = new LinkedBlockingQueue<>();
    private final Thread writeThread;
    private final LongAccumulator totalWriteBatches = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator totalWriteBuckets = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator acceptedBatches = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator acceptedBytes = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator bucketsCreated = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator totalWrite = new LongAccumulator(Long::sum, 0L);
    private final LongAccumulator timeSpentWriting = new LongAccumulator(Long::sum, 0L);

    public BulkIngestQueue(String identifier,
                           long minBucketSize,
                           long maxBucketSize,
                           int maxBatches,
                           long maxPendingWrite,
                           Duration maxDelay,
                           ScheduledExecutorService executorService,
                           Clock clock) {
        this.minBucketSize = minBucketSize;
        this.maxBucketSize = maxBucketSize;
        this.maxBatches = maxBatches;
        this.maxPendingWrite = maxPendingWrite;
        this.identifier = identifier;
        this.executorService = executorService;
        this.maxDelay = maxDelay;
        this.clock = clock;
        createNewBucket();
        this.writeThread = new Thread(this::processWriteQueue, "BulkIngestQueue-" + identifier + "-writer");
        this.writeThread.setDaemon(true);
        this.writeThread.start();
        executorService.schedule(this::triggerWriteIfRequired, maxDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void createNewBucket(){
        this.currentBucket = new Bucket<>(minBucketSize, maxBatches, maxDelay);
        bucketsCreated.accumulate(1);
    }


    private void processWriteQueue() {
        while (!terminating) {
            try {
                var task = writeQueue.take();

                // Try to combine with additional buckets from the queue
                var bucketsToCombine = new ArrayList<Bucket<T, R>>();
                bucketsToCombine.add(task.bucket());
                long combinedSize = task.bucket().size();
                int combinedBatchCount = task.bucket().batchCount();

                // Poll additional tasks while they can be combined
                WriteTask<T, R> nextTask;
                while ((nextTask = writeQueue.peek()) != null) {
                    var nextBucket = nextTask.bucket();
                    if (canCombine(combinedSize, combinedBatchCount, nextBucket, maxBucketSize, maxBatches)) {
                        writeQueue.poll(); // Remove from queue
                        bucketsToCombine.add(nextBucket);
                        combinedSize += nextBucket.size();
                        combinedBatchCount += nextBucket.batchCount();
                    } else {
                        break; // Can't combine more
                    }
                }

                // Create combined bucket if we have multiple, otherwise use original
                Bucket<T, R> bucketToWrite;
                if (bucketsToCombine.size() > 1) {
                    bucketToWrite = combineBuckets(bucketsToCombine, minBucketSize, maxBatches, maxDelay);
                } else {
                    bucketToWrite = task.bucket();
                }

                var combinedTask = new WriteTask<>(task.taskId(), task.startTime(), bucketToWrite);
                runningWrite = combinedTask;

                try {
                    var start = clock.instant();
                    write(combinedTask);
                    var end = clock.instant();
                    totalWriteBatches.accumulate(bucketToWrite.batches().size());
                    totalWrite.accumulate(bucketToWrite.size());
                    totalWriteBuckets.accumulate(bucketsToCombine.size());
                    timeSpentWriting.accumulate(Duration.between(start, end).toMillis());
                } catch (Exception e) {
                    // Complete futures with exception but continue processing remaining tasks
                    for (var future : bucketToWrite.futures()) {
                        if (!future.isDone()) {
                            future.completeExceptionally(e);
                        }
                    }
                    // Don't break - continue processing the next task in the queue
                } finally {
                    runningWrite = null;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Override
    public Stats getStats(){
        return new Stats(identifier, totalWrite.get(), totalWriteBatches.get(), totalWriteBuckets.get(), timeSpentWriting.get(),acceptedBatches.get() - totalWrite.get(), writeQueue.size());
    }

    public long getTotalWriteBatches() {
        return totalWriteBatches.get();
    }

    public long getTotalWriteBuckets() {
        return totalWriteBuckets.get();
    }

    public long getTotalWrite() {
        return totalWrite.get();
    }

    public long getTimeSpentWriting() {
        return timeSpentWriting.get();
    }

    @Override
    public String identifier() {
        return identifier;
    }

    @Override
    public long pendingWrite() {
        return acceptedBytes.get() - totalWrite.get();
    }

    /**
     * Calculates an estimated retry time based on the current ingestion rate.
     * Returns the time in seconds it would take to drain the current pending bytes.
     *
     * @param currentPending the current pending bytes
     * @return estimated retry time in seconds, bounded between 1 and 60 seconds
     */
    private int calculateRetryAfterSeconds(long currentPending) {
        long writtenBytes = totalWrite.get();
        long writingTimeMs = timeSpentWriting.get();

        // If no data has been written yet, use default
        if (writtenBytes == 0 || writingTimeMs == 0) {
            return 5; // Default 5 seconds
        }

        // Calculate write rate in bytes per millisecond
        double bytesPerMs = (double) writtenBytes / writingTimeMs;

        // Calculate estimated time to drain pending bytes (in seconds)
        double estimatedDrainSeconds = currentPending / bytesPerMs / 1000.0;

        // Bound between 1 and 60 seconds
        int retryAfterSeconds = (int) Math.ceil(estimatedDrainSeconds);
        return Math.max(1, Math.min(60, retryAfterSeconds));
    }

    public synchronized CompletableFuture<R> add(Batch<T> batch) {
        if (terminating) {
            throw new IllegalStateException("The queue is closed");
        }
        long currentPending = pendingWrite();
        if (currentPending + batch.totalSize() > maxPendingWrite) {
            int retryAfterSeconds = calculateRetryAfterSeconds(currentPending);
            return CompletableFuture.failedFuture(
                    new PendingWriteExceededException(currentPending, maxPendingWrite, retryAfterSeconds));
        }
        if (batch.producerId() != null) {
            var progressBatch = inProgressBatchIds.get(batch.producerId());
            if (progressBatch != null && progressBatch >= batch.producerBatchId()) {
                return CompletableFuture.failedFuture(
                        new OutOfSequenceBatch(progressBatch, batch.producerBatchId()));
            }
        }
        var result = new CompletableFuture<R>();
        currentBucket.add(batch, result);
        acceptedBatches.accumulate(1);
        acceptedBytes.accumulate(batch.totalSize());
        if (batch.producerId() != null) {
            inProgressBatchIds.put(batch.producerId(), batch.producerBatchId());
        }
        if (currentBucket.isFull()) {
           submitWriteTask();
        }
        return result;
    }

    private synchronized void triggerWriteIfRequired() {
        if (terminating) {
            return;
        }
        var now = clock.instant();
        if (currentBucket.isEmpty()) {
                scheduleNextTrigger(now);
                return;
        }

        var nextWrite = lastWrite.plus(maxDelay);
        if (currentBucket.isFull() || !nextWrite.isAfter(now)) {
            submitWriteTask();
        } else {
            scheduleNextTrigger(now);
        }
    }

    private void scheduleNextTrigger(Instant now) {
        var nextTrigger = lastWrite.plus(maxDelay);
        var timeRemaining = Duration.between(now, nextTrigger);
        executorService.schedule(this::triggerWriteIfRequired, Math.max(0, timeRemaining.toMillis()), TimeUnit.MILLISECONDS);
    }

    private synchronized void submitWriteTask() {
        if (currentBucket.isEmpty()) {
            return;
        }
        var toWrite = currentBucket;
        toWrite.markFinalized();
        createNewBucket();
        var writeTask = new WriteTask<>(writeTaskId++, clock.instant(), toWrite);
        lastWrite = clock.instant();
        writeQueue.offer(writeTask);
        if (!terminating) {
            scheduleNextTrigger(clock.instant());
        }
    }
    @Override
    public synchronized void close() throws Exception {
        terminating = true;
        var c = runningWrite;
        if (c != null) {
            c.cancel();
        }

        // Interrupt and wait for write thread to finish processing
        // The write thread will exit the loop when it sees terminating=true,
        // or when interrupted if it's blocked waiting for a task
        writeThread.interrupt();
        writeThread.join();

        // Fail any futures in the current bucket
        var exception = new IllegalStateException("Server shutting down before batch could be written");
        if (!currentBucket.isEmpty()) {
            for (var future : currentBucket.futures()) {
                future.completeExceptionally(exception);
            }
        }


        // Fail any remaining tasks that weren't processed
        WriteTask<T, R> remaining;
        while ((remaining = writeQueue.poll()) != null) {
            for (var future : remaining.bucket().futures()) {
                future.completeExceptionally(exception);
            }
        }
    }
}
