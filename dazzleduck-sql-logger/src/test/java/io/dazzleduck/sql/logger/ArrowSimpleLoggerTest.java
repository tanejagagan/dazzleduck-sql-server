package io.dazzleduck.sql.logger;

import io.dazzleduck.sql.common.ingestion.FlightSender;
import org.junit.jupiter.api.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ArrowSimpleLoggerTest {

    private ArrowSimpleLogger logger;
    private TestFlightSender sender;

    @BeforeEach
    void setup() {
        sender = new TestFlightSender();
        logger = new ArrowSimpleLogger("test-logger", sender);
    }

    @AfterEach
    void tearDown() throws Exception {
        logger.close();
    }

    @Test
    void testLoggerInitialization_ReadsConfig() {
        assertNotNull(logger.applicationId);
        assertNotNull(logger.applicationName);
        assertNotNull(logger.host);
    }

    @Test
    void testSingleLog_DoesNotFlush() {
        logger.info("hello {}", "world");

        assertEquals(1, logger.batchCounter.get());
        assertEquals(0, sender.sendCount.get());
    }

    @Test
    void testFlushOnBatchSize() throws Exception {
        for (int i = 0; i < 10; i++) {
            logger.info("log {}", i);
        }

        assertTrue(sender.awaitSends(1));
        assertEquals(1, sender.sendCount.get());
        assertEquals(0, logger.batchCounter.get());
    }

    @Test
    void testMultipleBatches() throws Exception {
        for (int i = 0; i < 25; i++) {
            logger.info("log {}", i);
        }

        assertTrue(sender.awaitSends(2));
        assertEquals(2, sender.sendCount.get());
        assertEquals(5, logger.batchCounter.get());
    }

    @Test
    void testFlushMethod() throws Exception {
        logger.info("one");
        logger.info("two");

        assertEquals(2, logger.batchCounter.get());

        logger.flush();

        assertTrue(sender.awaitSends(1));
        assertEquals(0, logger.batchCounter.get());
    }

    @Test
    void testCloseFlushes() throws Exception {
        logger.info("before close");

        logger.close();

        assertTrue(sender.sendCount.get() >= 1);
    }

    @Test
    void testLogWithLoggingEvent() {
        var event = org.mockito.Mockito.mock(org.slf4j.event.LoggingEvent.class);
        org.mockito.Mockito.when(event.getLevel()).thenReturn(org.slf4j.event.Level.INFO);
        org.mockito.Mockito.when(event.getMessage()).thenReturn("event msg");

        logger.log(event);

        assertEquals(1, logger.batchCounter.get());
    }

    @Test
    void testFormatReplacement() throws Exception {
        var m = ArrowSimpleLogger.class
                .getDeclaredMethod("format", String.class, Object[].class);
        m.setAccessible(true);

        String out = (String) m.invoke(logger, "A {} B {}", new Object[]{"X", 1});
        assertEquals("A X B 1", out);
    }

    /* ---------------- Test Sender ---------------- */

    static class TestFlightSender extends FlightSender.AbstractFlightSender {

        final AtomicInteger sendCount = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(10);

        @Override
        public long getMaxInMemorySize() {
            return 1024 * 1024;
        }

        @Override
        public long getMaxOnDiskSize() {
            return 10 * 1024 * 1024;
        }

        @Override
        protected void doSend(SendElement element) {
            sendCount.incrementAndGet();
            latch.countDown();
            try {
                element.read().close();
            } catch (Exception ignored) {
            }
        }

        boolean awaitSends(int expected) throws InterruptedException {
            return latch.await(2, TimeUnit.SECONDS)
                    || sendCount.get() >= expected;
        }
    }
}
