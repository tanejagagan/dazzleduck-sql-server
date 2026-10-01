package io.dazzleduck.sql.flight.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class StreamExecutorsTest {

    @Test
    void anInterruptedWaitStillWaitsForTheDuckDbCallToFinish() throws Exception {
        var pool = Executors.newSingleThreadExecutor();
        var executors = StreamExecutors.create(pool, pool);
        var callFinished = new AtomicBoolean();
        var callStarted = new CountDownLatch(1);
        var finishedBeforeReturn = new AtomicBoolean();
        var interruptRestored = new AtomicBoolean();
        Thread waiter = Thread.ofVirtual().start(() -> {
            try {
                executors.fetch(() -> {
                    callStarted.countDown();
                    Thread.sleep(500);
                    callFinished.set(true);
                    return null;
                });
                finishedBeforeReturn.set(callFinished.get());
                interruptRestored.set(Thread.currentThread().isInterrupted());
            } catch (Exception e) {
                fail(e);
            }
        });
        assertTrue(callStarted.await(5, TimeUnit.SECONDS));
        waiter.interrupt(); // like close()'s shutdownNow() while the stream waits for a batch
        waiter.join(10_000);
        assertTrue(finishedBeforeReturn.get(), "must not return (and close the result) while the native call runs");
        assertTrue(interruptRestored.get(), "the interrupt is kept for the caller");
        pool.shutdown();
    }

    @Test
    void afterShutdownQueriesAreRejectedButCleanupStillRuns() throws Exception {
        var pool = Executors.newSingleThreadExecutor();
        pool.shutdown();
        var executors = StreamExecutors.create(pool, pool);
        assertThrows(RejectedExecutionException.class, () -> executors.execute(() -> 1));
        assertThrows(RejectedExecutionException.class, () -> executors.fetch(() -> 1));
        var closed = new AtomicBoolean();
        executors.cleanup(() -> closed.set(true));
        assertTrue(closed.get(), "closing DuckDB resources still happens, inline");
    }

    @Test
    void fetchesDoNotWaitBehindLongExecutes() throws Exception {
        var executePool = Executors.newFixedThreadPool(1);
        var fetchPool = Executors.newFixedThreadPool(1);
        var executors = StreamExecutors.create(executePool, fetchPool);
        var release = new CountDownLatch(1);
        executePool.submit(() -> { release.await(); return null; }); // a long execute holds the whole pool
        try {
            long start = System.nanoTime();
            assertEquals(42, executors.fetch(() -> 42));
            assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2),
                    "a running stream's fetch must not queue behind executes");
        } finally {
            release.countDown();
            executePool.shutdown();
            fetchPool.shutdown();
        }
    }
}
