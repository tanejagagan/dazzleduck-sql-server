package io.dazzleduck.sql.common.ingestion;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class FlightSenderTest {

    private OnDemandSender sender;

    @AfterEach
    void cleanup() throws InterruptedException {
        if (sender != null) {
            try {
                sender.close();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
        sender = null;
    }

    private final long KB = 1024;
    private final long MB = 1024 * KB;

    private OnDemandSender createSender(long mem, long disk) {
        return new OnDemandSender(mem, disk, Clock.systemDefaultZone(), new CountDownLatch(1));
    }

    @Test
    void testStoreStatusFull() {
        sender = createSender(MB, 4 * MB);
        assertEquals(FlightSender.StoreStatus.ON_DISK, sender.getStoreStatus((int) MB));
        assertEquals(FlightSender.StoreStatus.FULL, sender.getStoreStatus((int) (5 * MB)));
    }

    @Test
    void testEnqueueInMemory() {
        sender = createSender(10 * MB, 10 * MB);
        // Thread is auto-started in constructor, no need to call start()
        assertDoesNotThrow(() -> sender.enqueue(new byte[1024]));
    }

    @Test
    void testEnqueueOnDisk() {
        sender = createSender(100, 10 * MB);
        // Thread is auto-started in constructor, no need to call start()
        // assertDoesNotThrow(() -> sender.enqueue(new byte[1024]));
        sender.enqueue(new byte[1024]);

    }

    @Test
    void testEnqueueThrowsWhenFull() {
        sender = createSender(MB, 5 * MB);
        // Thread is auto-started in constructor, no need to call start()
        sender.enqueue(new byte[(int) MB]); // goes to disk
        sender.release(); // let it process
        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> sender.enqueue(new byte[(int) (6 * MB)])
        );

        assertEquals("queue is full", ex.getMessage());
    }

    @Test
    void testFileCleanupAfterProcessing() throws Exception {
        sender = new OnDemandSender(100, 10 * MB, Clock.systemDefaultZone(), new CountDownLatch(1));
        // Thread is auto-started in constructor, no need to call start()
        sender.enqueue(new byte[1024]);  // goes to ON_DISK
        assertEquals(1, sender.filesCreated.get());
        sender.release();
        Thread.sleep(60);
        assertEquals(1, sender.filesDeleted.get());
    }

    @Test
    void testFileCleanupOnEnqueueFailure() {
        sender = new OnDemandSender(100, 1024, Clock.systemDefaultZone(), new CountDownLatch(1));

        sender.enqueue(new byte[500]); // fits in ON_DISK
        assertEquals(1, sender.filesCreated.get());
        assertThrows(IllegalStateException.class, () -> sender.enqueue(new byte[555]));
        assertEquals(2, sender.filesCreated.get());
        assertEquals(1, sender.filesDeleted.get());
    }

    @Test
    void testCloseInterruptsInFlightProcessing() throws Exception {
        CountDownLatch blockLatch = new CountDownLatch(1);
        sender = new OnDemandSender(10 * MB, 10 * MB, Clock.systemDefaultZone(), blockLatch);
        // Thread is auto-started in constructor, no need to call start()
        sender.enqueue(new byte[1024]);

        sender.close();
        assertEquals(0, sender.filesDeleted.get());
        blockLatch.countDown();
    }


    class OnDemandSender extends FlightSender.AbstractFlightSender {

    private final long maxOnDiskSize;
    private final long maxInMemorySize;
    private final CountDownLatch latch;
    public final AtomicInteger filesCreated = new AtomicInteger();
    public final AtomicInteger filesDeleted = new AtomicInteger();

    public OnDemandSender(long maxInMemorySize, long maxOnDiskSize, Clock clock, CountDownLatch latch) {
        super(1024 * 1024, Duration.ofSeconds(1), clock);
        this.maxInMemorySize = maxInMemorySize;
        this.maxOnDiskSize = maxOnDiskSize;
        this.latch = latch;
    }

    @Override
    public long getMaxInMemorySize() {
        return maxInMemorySize;
    }

    @Override
    public long getMaxOnDiskSize() {
        return maxOnDiskSize;
    }

        @Override
        public void enqueue(byte[] input) {
            filesCreated.incrementAndGet(); // always count
            try {
                super.enqueue(input);
            } catch (Exception e) {
                filesDeleted.incrementAndGet(); // rollback
                throw e;
            }
        }

        @Override
        protected void doSend(SendElement element) throws InterruptedException {
            latch.await();        // wait for release()
            filesDeleted.incrementAndGet(); // after successful send
        }
        public void release() {
            latch.countDown();
        }
    }
}