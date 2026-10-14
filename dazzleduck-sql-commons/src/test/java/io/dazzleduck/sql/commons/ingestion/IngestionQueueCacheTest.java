package io.dazzleduck.sql.commons.ingestion;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A server calls {@link IngestionHandler#getOrCreateQueue} once per request. Each queue owns a
 * writer thread, so a handler that builds a new queue per call leaks a thread per request — which
 * the default handler, {@link NOOPIngestionTaskFactoryProvider}'s, used to do. These tests use real
 * queues and count their live writer threads, the resource that actually leaked.
 */
class IngestionQueueCacheTest {

    @TempDir
    Path dir;

    private final DeterministicScheduler scheduler = new DeterministicScheduler();
    private final AtomicInteger created = new AtomicInteger();
    private final AtomicInteger createdEvents = new AtomicInteger();
    private final AtomicInteger deletedEvents = new AtomicInteger();

    private final IngestionHandler.QueueEventListener listener = new IngestionHandler.QueueEventListener() {
        @Override public void onCreated(String queueId) { createdEvents.incrementAndGet(); }
        @Override public void onDeleted(String queueId) { deletedEvents.incrementAndGet(); }
    };

    private IngestionHandler.QueueCreator creator(IngestionHandler handler) {
        return (id, path) -> {
            created.incrementAndGet();
            return new ParquetIngestionQueue("test-app", "arrow", path, id,
                    1024, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, Duration.ofSeconds(5),
                    handler, scheduler, Clock.systemUTC());
        };
    }

    private static long liveWriterThreads(String queueId) {
        String name = "BulkIngestQueue-" + queueId + "-writer";
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.isAlive() && t.getName().equals(name))
                .count();
    }

    @Test
    void noopHandlerReturnsOneQueuePerIdAcrossRequests() {
        IngestionHandler handler = new NOOPIngestionTaskFactoryProvider(dir.toString()).getIngestionHandler();
        IngestionHandler.QueueCreator creator = creator(handler);
        try {
            ParquetIngestionQueue first = handler.getOrCreateQueue("logs", creator, listener);
            for (int request = 0; request < 50; request++) {
                assertSame(first, handler.getOrCreateQueue("logs", creator, listener), "request " + request);
            }
            ParquetIngestionQueue other = handler.getOrCreateQueue("metrics", creator, listener);
            assertNotSame(first, other);

            assertEquals(2, created.get(), "one queue per id, not one per request");
            assertEquals(2, createdEvents.get());
            assertEquals(1, liveWriterThreads("logs"));
            assertEquals(2, handler.getQueueStats().size());
        } finally {
            handler.closeQueues(Duration.ofSeconds(5));
        }
    }

    @Test
    void noopHandlerClosesItsQueuesOnShutdown() {
        IngestionHandler handler = new NOOPIngestionTaskFactoryProvider(dir.toString()).getIngestionHandler();
        IngestionHandler.QueueCreator creator = creator(handler);
        ParquetIngestionQueue queue = handler.getOrCreateQueue("logs", creator, listener);
        assertEquals(1, liveWriterThreads("logs"));

        handler.closeQueues(Duration.ofSeconds(5));

        assertEquals(0, liveWriterThreads("logs"), "the writer thread must stop when the queue is closed");
        assertTrue(handler.getQueueStats().isEmpty());
        // A request after shutdown gets a fresh queue rather than the closed one.
        try {
            assertNotSame(queue, handler.getOrCreateQueue("logs", creator, listener));
        } finally {
            handler.closeQueues(Duration.ofSeconds(5));
        }
    }

    @Test
    void concurrentRequestsCreateOneQueue() throws Exception {
        IngestionHandler handler = new NOOPIngestionTaskFactoryProvider(dir.toString()).getIngestionHandler();
        IngestionHandler.QueueCreator creator = creator(handler);
        int threads = 16;
        CountDownLatch start = new CountDownLatch(1);
        Set<ParquetIngestionQueue> seen = ConcurrentHashMap.newKeySet();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < 100; i++) {
                        seen.add(handler.getOrCreateQueue("logs", creator, listener));
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) f.get();

            assertEquals(1, seen.size());
            assertEquals(1, created.get());
            assertEquals(1, liveWriterThreads("logs"));
        } finally {
            pool.shutdownNow();
            handler.closeQueues(Duration.ofSeconds(5));
        }
    }

    @Test
    void goneTargetPathEvictsAndClosesTheQueue() {
        IngestionHandler handler = new NOOPIngestionTaskFactoryProvider(dir.toString()).getIngestionHandler();
        IngestionQueueCache cache = new IngestionQueueCache();
        IngestionHandler.QueueCreator creator = creator(handler);
        try {
            cache.getOrCreate("logs", dir.resolve("logs").toString(), creator, listener);
            assertEquals(1, liveWriterThreads("logs"));

            assertNull(cache.getOrCreate("logs", null, creator, listener));
            assertEquals(0, liveWriterThreads("logs"));
            assertEquals(0, cache.size());
            assertEquals(1, deletedEvents.get());

            // An id that was never cached has nothing to delete, so no event.
            assertNull(cache.getOrCreate("never-seen", null, creator, listener));
            assertEquals(1, deletedEvents.get());
            assertFalse(cache.evict("never-seen"));
        } finally {
            cache.closeAll(Duration.ofSeconds(5));
        }
    }
}
