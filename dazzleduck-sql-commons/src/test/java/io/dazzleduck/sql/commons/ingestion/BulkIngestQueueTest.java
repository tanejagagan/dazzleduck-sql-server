package io.dazzleduck.sql.commons.ingestion;


import io.dazzleduck.sql.commons.util.MutableClock;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jmock.lib.concurrent.DeterministicScheduler;

import static org.junit.jupiter.api.Assertions.*;

public class BulkIngestQueueTest {
    public static final int DEFAULT_SMALL_BATCH_SIZE = 1024;
    private static final long DEFAULT_MIN_BATCH_SIZE = 10 * DEFAULT_SMALL_BATCH_SIZE;
    private static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(5);
    @Test
    public void testSingleLargeBatchBiggerThanBucket() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            var res = queue.add(mockBatch("123",
                    0, DEFAULT_MIN_BATCH_SIZE + 1));
            service.tick(1, TimeUnit.MILLISECONDS);
            Thread.sleep(5);
            assertTrue(res.isDone());
            assertEquals(new MockWriteResult(0, DEFAULT_MIN_BATCH_SIZE + 1), res.get());
        });
    }

    @Test
    public void testSmallBatchToFillTheBucket() throws Exception {
        var list = new ArrayList<Future<MockWriteResult>>();
        var numBatches = 10;
        withServiceAndQueue((service, queue, clock) -> {
            for (int i = 0; i < numBatches; i++) {
                var res = queue.add(mockBatch("123",
                        i, DEFAULT_SMALL_BATCH_SIZE));
                list.add(res);
            }
            service.tick(1, TimeUnit.MILLISECONDS);

            Thread.sleep(5);
            // test schedule write
            var stat = queue.getStats();
            assertEquals(0 , stat.pendingBuckets());
            for (int i = 0; i < numBatches; i++) {
                var f = list.get(i);
                assertTrue(f.isDone());
                assertEquals(new MockWriteResult(0, numBatches * DEFAULT_SMALL_BATCH_SIZE), f.get());
            }
        });
    }

    @Test
    @Disabled
    public void testSmallBatchesToFillTheBucketAndSomeSpace() throws Exception {
        var list = new ArrayList<Future<MockWriteResult>>();
        var numBatches = 25;
        withServiceAndQueue((service, queue, clock) -> {
            for (int i = 0; i < numBatches; i++) {
                var res = queue.add(mockBatch("123",
                        i, DEFAULT_SMALL_BATCH_SIZE));
                list.add(res);
            }
            service.tick(1, TimeUnit.MILLISECONDS);
            var stat = queue.getStats();
            clock.advanceBy(DEFAULT_MAX_DELAY.plusMillis(10));
            service.tick(DEFAULT_MAX_DELAY.toMillis() + 10, TimeUnit.MILLISECONDS);

            Thread.sleep(10);
            // test schedule write
            var newStat = queue.getStats();
            assertEquals(0, stat.pendingBuckets());
            assertEquals(2, stat.totalWriteBatches());

            assertEquals(1, newStat.pendingBatches());
            assertEquals(3, newStat.totalWriteBatches());
            for (int i = 0; i < numBatches; i++) {
                var f = list.get(i);
                assertTrue(f.isDone());
            }
        });
    }

    @Test
    public void testSingleFewBatchWithSpaceInTheBucket() throws Exception {
        var list = new ArrayList<Future<MockWriteResult>>();
        var numBatches = 5;
        withServiceAndQueue((service, queue, clock) -> {
            for (int i = 0; i < numBatches; i++) {
                var res = queue.add(mockBatch("123",
                        i, DEFAULT_SMALL_BATCH_SIZE));
                list.add(res);
            }

            //service.tick(1, TimeUnit.MILLISECONDS);
            for (int i = 0; i < numBatches; i++) {
                assertFalse(list.get(i).isDone());
            }
            clock.advanceBy(DEFAULT_MAX_DELAY.plusMillis(10));
            service.tick(DEFAULT_MAX_DELAY.toMillis() + 10, TimeUnit.MILLISECONDS);
            var stat = queue.getStats();
            // schedule write
            Thread.sleep(5);
            //assertEquals(1, stat.scheduledWrite());
            for (int i = 0; i < numBatches; i++) {
                var f = list.get(i);
                assertTrue(f.isDone());
                //assertEquals(new MockWriteResult(0, numBatches * DEFAULT_SMALL_BATCH_SIZE), f.get());
            }
        });
    }

    @Test
    public void testSmallLargeBatchToFillTheBucket() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            var smallBatch = queue.add(mockBatch("123", 0, DEFAULT_SMALL_BATCH_SIZE));
            var largeBatch = queue.add(mockBatch("124", 1, 11 * DEFAULT_SMALL_BATCH_SIZE));
            service.tick(1, TimeUnit.MILLISECONDS);
            Thread.sleep(2);
            assertTrue(smallBatch.isDone());
            assertEquals(new MockWriteResult(0, 12 * DEFAULT_SMALL_BATCH_SIZE), smallBatch.get());
            assertEquals(new MockWriteResult(0, 12 * DEFAULT_SMALL_BATCH_SIZE), largeBatch.get());
        });
    }

    @Test
    public void testDrainFlushesPendingBatches() throws Exception {
        var list = new ArrayList<Future<MockWriteResult>>();
        var numBatches = 5; // below min bucket size and maxBatches, so the bucket stays buffered
        withServiceAndQueue((service, queue, clock) -> {
            for (int i = 0; i < numBatches; i++) {
                list.add(queue.add(mockBatch("123", i, DEFAULT_SMALL_BATCH_SIZE)));
            }
            // No tick and the bucket is not full, so nothing has been written yet.
            for (var f : list) {
                assertFalse(f.isDone());
            }

            queue.drain();

            // drain() guarantees every accepted batch is written and its future completed.
            for (var f : list) {
                assertTrue(f.isDone());
                assertNotNull(f.get());
            }
            assertEquals(numBatches, queue.getStats().totalWriteBatches());
            assertEquals(0, queue.getStats().pendingBatches());
            assertEquals(0, queue.pendingWrite());

            // A drained queue no longer accepts work.
            assertThrows(IllegalStateException.class,
                    () -> queue.add(mockBatch("123", 99, DEFAULT_SMALL_BATCH_SIZE)));
        });
    }

    @Test
    public void testDrainEmptyQueueDoesNotHang() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            // Nothing was ever added; drain must still return promptly.
            queue.drain();
            assertEquals(0, queue.getStats().totalWriteBatches());
            // Idempotent: a second drain (and a following close) are safe.
            queue.drain();
            queue.close();
        });
    }

    @Test
    public void testCloseAfterDrainIsClean() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            var f = queue.add(mockBatch("123", 0, DEFAULT_SMALL_BATCH_SIZE));
            queue.drain();
            assertTrue(f.isDone());
            assertNotNull(f.get());
            // close() after a successful drain finds nothing to abandon.
            queue.close();
            assertTrue(f.isDone());
        });
    }

    @Test
    public void testDrainWithTimeoutCompletes() throws Exception {
        withServiceAndQueue((service, queue, clock) -> {
            var f = queue.add(mockBatch("123", 0, DEFAULT_SMALL_BATCH_SIZE));
            // Writes complete promptly, so a generous bound returns true well before it elapses.
            assertTrue(queue.drain(Duration.ofSeconds(5)));
            assertTrue(f.isDone());
            assertNotNull(f.get());
        });
    }

    @Test
    public void testDrainWithTimeoutExpiresThenCloseReleases() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var release = new java.util.concurrent.CountDownLatch(1);
        var queue = new BlockingMockQueue(release, service, clock);

        var f = queue.add(mockBatch("123", 0, DEFAULT_SMALL_BATCH_SIZE));

        // The writer is stuck inside write(), so a short bound elapses and drain reports failure
        // without hanging.
        long start = System.nanoTime();
        boolean completed = queue.drain(Duration.ofMillis(100));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertFalse(completed);
        assertFalse(f.isDone());
        assertTrue(elapsedMs < 5_000, "drain(timeout) must not block past its bound");

        // Unblock the write so the in-flight task can finish, then close() releases cleanly.
        release.countDown();
        queue.close();
        assertTrue(f.isDone());
    }

    @Test
    public void testCloseDoesNotHangOnStuckWriter() throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var release = new java.util.concurrent.CountDownLatch(1);
        var queue = new BlockingMockQueue(release, service, clock);

        try {
            queue.add(mockBatch("123", 0, DEFAULT_SMALL_BATCH_SIZE));
            // drain submits the bucket; the writer enters write() and gets stuck (and ignores the
            // interrupt close() will send, mimicking a native COPY that doesn't respond to cancellation).
            queue.drain(Duration.ofMillis(50));
            queue.started.await();

            long start = System.nanoTime();
            queue.close(); // must return within the bounded join, not block on the stuck writer
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertTrue(elapsedMs < 5_000, "close() must not hang on a stuck writer");
        } finally {
            // Always let the abandoned writer thread finish and exit, even if an assertion fails.
            release.countDown();
        }
    }

    /** Queue whose write() blocks until a latch is released — used to exercise drain/close timeouts. */
    private static final class BlockingMockQueue extends BulkIngestQueue<String, MockWriteResult> {
        private final java.util.concurrent.CountDownLatch release;
        final java.util.concurrent.CountDownLatch started = new java.util.concurrent.CountDownLatch(1);

        BlockingMockQueue(java.util.concurrent.CountDownLatch release,
                          ScheduledExecutorService executorService, Clock clock) {
            super("", DEFAULT_MIN_BATCH_SIZE, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE,
                    DEFAULT_MAX_DELAY, executorService, clock);
            this.release = release;
        }

        // Short bound so the stuck-writer test doesn't wait the production 5s.
        @Override
        protected Duration closeJoinTimeout() {
            return Duration.ofMillis(200);
        }

        @Override
        public void write(WriteTask<String, MockWriteResult> writeTask) {
            started.countDown();
            // Wait uninterruptibly — a native write (e.g. DuckDB COPY) does not unblock on
            // Thread.interrupt(), so the close() bound, not the interrupt, must cap the wait.
            boolean released = false;
            while (!released) {
                try {
                    release.await();
                    released = true;
                } catch (InterruptedException e) {
                    // swallow and keep waiting, mimicking an uninterruptible native call
                }
            }
            for (var future : writeTask.bucket().futures()) {
                future.complete(new MockWriteResult(writeTask.taskId(), writeTask.size()));
            }
        }
    }

    private MockBulkIngestQueue createMockQueue(ScheduledExecutorService executorService, Clock clock) {
        return new MockBulkIngestQueue("", DEFAULT_MIN_BATCH_SIZE, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, DEFAULT_MAX_DELAY,
                executorService,
                clock);
    }

    private void withServiceAndQueue(ServiceAndQueue serviceAndQueue) throws Exception {
        var service = new DeterministicScheduler();
        var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
        var queue = createMockQueue(service, clock);
        serviceAndQueue.apply(service, queue, clock);
    }

    private Batch<String> mockBatch(String producerId, long producerBatchId, long totalSize) {
        return new Batch<String>(new String[0], new String[0], "", producerId, producerBatchId, totalSize, "parquet",Instant.now());
    }

    interface ServiceAndQueue {
        void apply(DeterministicScheduler service, MockBulkIngestQueue queue, MutableClock clock) throws Exception;
    }

    // Tests for combineBuckets static method

    @Test
    public void testCombineBucketsWithMultipleBuckets() {
        // Create three buckets with different batches
        var bucket1 = new Bucket<String, MockWriteResult>(DEFAULT_MIN_BATCH_SIZE, Integer.MAX_VALUE, DEFAULT_MAX_DELAY);
        var bucket2 = new Bucket<String, MockWriteResult>(DEFAULT_MIN_BATCH_SIZE, Integer.MAX_VALUE, DEFAULT_MAX_DELAY);
        var bucket3 = new Bucket<String, MockWriteResult>(DEFAULT_MIN_BATCH_SIZE, Integer.MAX_VALUE, DEFAULT_MAX_DELAY);

        var future1 = new CompletableFuture<MockWriteResult>();
        var future2 = new CompletableFuture<MockWriteResult>();
        var future3 = new CompletableFuture<MockWriteResult>();

        bucket1.add(mockBatch("p1", 0, 100), future1);
        bucket2.add(mockBatch("p2", 0, 200), future2);
        bucket3.add(mockBatch("p3", 0, 300), future3);

        var buckets = List.of(bucket1, bucket2, bucket3);
        var combined = BulkIngestQueue.combineBuckets(buckets, DEFAULT_MIN_BATCH_SIZE, Integer.MAX_VALUE, DEFAULT_MAX_DELAY);

        assertEquals(600, combined.size());
        assertEquals(3, combined.batchCount());
        assertEquals(3, combined.futures().size());
        assertTrue(combined.futures().contains(future1));
        assertTrue(combined.futures().contains(future2));
        assertTrue(combined.futures().contains(future3));
    }

    @Test
    public void testCombineBucketsWithSingleBucket() {
        var bucket = new Bucket<String, MockWriteResult>(DEFAULT_MIN_BATCH_SIZE, Integer.MAX_VALUE, DEFAULT_MAX_DELAY);
        var future = new CompletableFuture<MockWriteResult>();
        bucket.add(mockBatch("p1", 0, 500), future);

        var buckets = List.of(bucket);
        var combined = BulkIngestQueue.combineBuckets(buckets, DEFAULT_MIN_BATCH_SIZE, Integer.MAX_VALUE, DEFAULT_MAX_DELAY);

        assertEquals(500, combined.size());
        assertEquals(1, combined.batchCount());
        assertEquals(1, combined.futures().size());
        assertSame(future, combined.futures().get(0));
    }

    @Test
    public void testCombineBucketsWithEmptyList() {
        List<Bucket<String, MockWriteResult>> buckets = Collections.emptyList();
        var combined = BulkIngestQueue.combineBuckets(buckets, DEFAULT_MIN_BATCH_SIZE, Integer.MAX_VALUE, DEFAULT_MAX_DELAY);

        assertEquals(0, combined.size());
        assertEquals(0, combined.batchCount());
        assertTrue(combined.futures().isEmpty());
    }

    @Test
    public void testCombineBucketsPreservesBatchOrder() {
        var bucket1 = new Bucket<String, MockWriteResult>(DEFAULT_MIN_BATCH_SIZE, Integer.MAX_VALUE, DEFAULT_MAX_DELAY);
        var bucket2 = new Bucket<String, MockWriteResult>(DEFAULT_MIN_BATCH_SIZE, Integer.MAX_VALUE, DEFAULT_MAX_DELAY);

        var future1 = new CompletableFuture<MockWriteResult>();
        var future2 = new CompletableFuture<MockWriteResult>();
        var future3 = new CompletableFuture<MockWriteResult>();

        var batch1 = mockBatch("p1", 0, 100);
        var batch2 = mockBatch("p1", 1, 200);
        var batch3 = mockBatch("p2", 0, 300);

        bucket1.add(batch1, future1);
        bucket1.add(batch2, future2);
        bucket2.add(batch3, future3);

        var buckets = List.of(bucket1, bucket2);
        var combined = BulkIngestQueue.combineBuckets(buckets, DEFAULT_MIN_BATCH_SIZE, Integer.MAX_VALUE, DEFAULT_MAX_DELAY);

        assertEquals(3, combined.batchCount());
        assertSame(batch1, combined.batches().get(0));
        assertSame(batch2, combined.batches().get(1));
        assertSame(batch3, combined.batches().get(2));
    }

    @Test
    public void testCombineBucketsWithMultipleBatchesPerBucket() {
        var bucket1 = new Bucket<String, MockWriteResult>(DEFAULT_MIN_BATCH_SIZE, Integer.MAX_VALUE, DEFAULT_MAX_DELAY);
        var bucket2 = new Bucket<String, MockWriteResult>(DEFAULT_MIN_BATCH_SIZE, Integer.MAX_VALUE, DEFAULT_MAX_DELAY);

        // Add multiple batches to each bucket
        for (int i = 0; i < 5; i++) {
            bucket1.add(mockBatch("p1", i, 100), new CompletableFuture<>());
        }
        for (int i = 0; i < 3; i++) {
            bucket2.add(mockBatch("p2", i, 200), new CompletableFuture<>());
        }

        var buckets = List.of(bucket1, bucket2);
        var combined = BulkIngestQueue.combineBuckets(buckets, DEFAULT_MIN_BATCH_SIZE, Integer.MAX_VALUE, DEFAULT_MAX_DELAY);

        assertEquals(500 + 600, combined.size()); // 5*100 + 3*200
        assertEquals(8, combined.batchCount()); // 5 + 3
        assertEquals(8, combined.futures().size());
    }
}
