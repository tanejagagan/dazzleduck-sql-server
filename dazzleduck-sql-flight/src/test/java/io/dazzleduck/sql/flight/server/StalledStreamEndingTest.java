package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * How a stream that is waiting for a client that stopped reading ends when the server, not the
 * client, ends it. It must end with an error status: completing it would tell the client a
 * cut-off result was the whole result.
 */
@Timeout(30)
class StalledStreamEndingTest {

    private static final String ENDLESS = "SELECT * FROM range(10000000000)";

    private BufferAllocator allocator;
    private ExecutorService streams;

    @BeforeEach
    void setup() {
        allocator = new RootAllocator();
        streams = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void teardown() throws Exception {
        streams.shutdownNow();
        assertTrue(streams.awaitTermination(10, TimeUnit.SECONDS));
        allocator.close();
    }

    /** A client that has stopped reading: never ready. */
    private static final class StalledListener implements FlightProducer.ServerStreamListener {
        final CountDownLatch waiting = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(1);
        volatile Throwable error;
        volatile boolean completed;

        @Override public boolean isCancelled() { return false; }
        @Override public void setOnCancelHandler(Runnable handler) { }
        @Override public void setOnReadyHandler(Runnable handler) { }
        @Override public boolean isReady() { waiting.countDown(); return false; }
        @Override public void start(org.apache.arrow.vector.VectorSchemaRoot root,
                                    org.apache.arrow.vector.dictionary.DictionaryProvider dictionaries,
                                    org.apache.arrow.vector.ipc.message.IpcOption option) { }
        @Override public void putNext() { }
        @Override public void putNext(org.apache.arrow.memory.ArrowBuf metadata) { }
        @Override public void putMetadata(org.apache.arrow.memory.ArrowBuf metadata) { }
        @Override public void error(Throwable ex) { error = ex; done.countDown(); }
        @Override public void completed() { completed = true; done.countDown(); }
    }

    private static void assertEndedWith(StalledListener listener, CallStatus status) throws InterruptedException {
        assertTrue(listener.done.await(10, TimeUnit.SECONDS), "the stream must end");
        assertFalse(listener.completed, "a cut-off result must not be reported as complete");
        var error = assertInstanceOf(FlightRuntimeException.class, listener.error);
        assertEquals(status.code(), error.status().code(), String.valueOf(error));
    }

    @Test
    void aStatementStreamInterruptedByShutdownEndsWithAnError() throws Exception {
        var connection = ConnectionPool.getConnection();
        var ctx = new StatementContext<>(connection, connection.createStatement(), ENDLESS);
        var listener = new StalledListener();
        var cleanedUp = new CountDownLatch(1);
        ctx.markClaimed();
        ResultSetStreamUtil.streamResultSet(StreamExecutors.sameThread(streams), ctx,
                new DuckDBFlightSqlProducer.CacheKey("admin", 1L), OptionalResultSetSupplier.of(ctx.getStatement(), ENDLESS),
                allocator, 1024, listener, () -> { ctx.close(); cleanedUp.countDown(); },
                new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test"));
        assertTrue(listener.waiting.await(10, TimeUnit.SECONDS));

        streams.shutdownNow(); // close()'s fallback, without a cancel first

        assertEndedWith(listener, CallStatus.UNAVAILABLE);
        assertTrue(cleanedUp.await(10, TimeUnit.SECONDS), "the stream gives back its connection");
    }

    @Test
    void aMetadataStreamInterruptedByShutdownEndsWithAnError() throws Exception {
        var connection = ConnectionPool.getConnection();
        var listener = new StalledListener();
        ResultSetStreamUtil.streamResultSet(StreamExecutors.sameThread(streams),
                () -> (DuckDBResultSet) connection.createStatement().executeQuery(ENDLESS),
                allocator, 1024, listener, () -> {
                    try { connection.close(); } catch (Exception ignored) { }
                },
                new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test"));
        assertTrue(listener.waiting.await(10, TimeUnit.SECONDS));

        streams.shutdownNow();

        assertEndedWith(listener, CallStatus.UNAVAILABLE);
    }

    @Test
    void aServerSideCancelEndsAStreamWaitingForItsClient() throws Exception {
        var connection = ConnectionPool.getConnection();
        var ctx = new StatementContext<>(connection, connection.createStatement(), ENDLESS);
        var listener = new StalledListener();
        var cleanedUp = new CountDownLatch(1);
        ctx.markClaimed();
        ResultSetStreamUtil.streamResultSet(StreamExecutors.sameThread(streams), ctx,
                new DuckDBFlightSqlProducer.CacheKey("admin", 2L), OptionalResultSetSupplier.of(ctx.getStatement(), ENDLESS),
                allocator, 1024, listener, () -> { ctx.close(); cleanedUp.countDown(); },
                new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test"));
        assertTrue(listener.waiting.await(10, TimeUnit.SECONDS));

        assertEquals(StatementContext.CancelOutcome.INTERRUPTED, ctx.cancel()); // like CancelFlightInfo

        // Without the client reading again: the stream notices on its next recheck and ends,
        // giving back its connection.
        assertEndedWith(listener, CallStatus.CANCELLED);
        assertTrue(cleanedUp.await(10, TimeUnit.SECONDS), "the stream gives back its connection");
    }
}
