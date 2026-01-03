package io.dazzleduck.sql.client;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FlightProducerTest {

    private static final long KB = 1024;
    private static final long MB = 1024 * KB;

    private OnDemandProducer sender;

    @AfterEach
    void cleanup() throws InterruptedException {
        if (sender != null) {
            sender.close();
        }
    }

    private OnDemandProducer createSender(long mem, long disk) {
        return new OnDemandProducer(mem, disk, Clock.systemDefaultZone(), new CountDownLatch(1),null);
    }

    @Test
    void testStoreStatusFull() {
        sender = createSender(MB, 4 * MB);
        assertEquals(FlightProducer.StoreStatus.ON_DISK, sender.getStoreStatus((int) MB));
        assertEquals(FlightProducer.StoreStatus.FULL, sender.getStoreStatus((int) (5 * MB)));
    }

    @Test
    void testEnqueueInMemory() {
        sender = createSender(10 * MB, 10 * MB);
        assertDoesNotThrow(() -> sender.enqueue(new byte[1024]));
    }

    @Test
    void testEnqueueOnDisk() {
        sender = createSender(100, 10 * MB);
        sender.enqueue(new byte[1024]);
        assertEquals(1, sender.filesCreated.get());
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
        CountDownLatch sendDone = new CountDownLatch(1);
        sender = new OnDemandProducer(100, 10 * MB, Clock.systemUTC(), new CountDownLatch(1), sendDone
        );

        sender.enqueue(new byte[1024]);
        assertEquals(1, sender.filesCreated.get());

        sender.release(); // allow doSend()

        assertTrue(sendDone.await(2, TimeUnit.SECONDS));
        assertEquals(1, sender.filesDeleted.get());
    }

    @Test
    void testFileCleanupOnEnqueueFailure() {
        sender = createSender(100, 1024);

        sender.enqueue(new byte[500]); // fits in ON_DISK
        assertEquals(1, sender.filesCreated.get());
        assertThrows(IllegalStateException.class, () -> sender.enqueue(new byte[555]));
        assertEquals(2, sender.filesCreated.get());
        assertEquals(1, sender.filesDeleted.get());
    }

    @Test
    void testCloseInterruptsInFlightProcessing() throws Exception {
        CountDownLatch blockLatch = new CountDownLatch(1);
        sender = new OnDemandProducer(10 * MB, 10 * MB, Clock.systemDefaultZone(), blockLatch,null);
        // Thread is auto-started in constructor, no need to call start()
        sender.enqueue(new byte[1024]);

        sender.close();
        assertEquals(0, sender.filesDeleted.get());
        blockLatch.countDown();
    }


    static class OnDemandProducer extends FlightProducer.AbstractFlightProducer {

    private final long maxOnDiskSize;
    private final long maxInMemorySize;
    private final CountDownLatch latch;
    public final AtomicInteger filesCreated = new AtomicInteger();
    public final AtomicInteger filesDeleted = new AtomicInteger();
    private final CountDownLatch sendDone;

        OnDemandProducer(long maxInMemorySize, long maxOnDiskSize, Clock clock, CountDownLatch blockLatch, CountDownLatch sendDone) {
            super(1024 * 1024, Duration.ofSeconds(1),  new Schema(java.util.List.of(new Field("ts", FieldType.nullable(new ArrowType.Utf8()), null))), clock, 3, 1000, java.util.List.of(), java.util.List.of());
            this.maxInMemorySize = maxInMemorySize;
            this.maxOnDiskSize = maxOnDiskSize;
            this.latch = blockLatch;
            this.sendDone = sendDone;
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
            latch.await();
            filesDeleted.incrementAndGet();
            if (sendDone != null) {
                sendDone.countDown();
            }
        }

        void release() {
            latch.countDown();
        }
    }
}