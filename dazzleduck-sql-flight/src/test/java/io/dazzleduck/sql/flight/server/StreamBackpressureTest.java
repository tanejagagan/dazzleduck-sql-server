package io.dazzleduck.sql.flight.server;

import com.google.protobuf.ByteString;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.authorization.SubjectAndVerifiedClaims;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.context.SyntheticFlightContext;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBResultSet;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.BiFunction;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The server must not produce results faster than the client takes them, must stop a query whose
 * HTTP client went away, and must end a stream waiting for a stalled client with an error when the
 * server stops it (shutdown, server-side cancel).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(60)
class StreamBackpressureTest {

    private static final String ENDLESS_STREAM = "SELECT * FROM range(10000000000)";

    private BufferAllocator allocator;
    private FlightServer server;
    private FlightSqlClient client;
    private DuckDBFlightSqlProducer producer;

    @BeforeAll
    void setup() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        Location location = FlightTestUtils.findNextLocation();
        producer = new DuckDBFlightSqlProducer(
                location, UUID.randomUUID().toString(), "test-secret", allocator,
                System.getProperty("java.io.tmpdir"), AccessMode.COMPLETE, DuckDBFlightSqlProducer.newTempDir(),
                null, Executors.newSingleThreadScheduledExecutor(), Duration.ofMinutes(10), Duration.ZERO,
                Clock.systemDefaultZone(), new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test"),
                DuckDBFlightSqlProducer.DEFAULT_INGESTION_CONFIG, List.of());
        server = FlightServer.builder(allocator, location, producer)
                .headerAuthenticator(AuthUtils.getTestAuthenticator())
                .build()
                .start();
        client = new FlightSqlClient(FlightClient.builder(allocator, location)
                .intercept(AuthUtils.createClientMiddlewareFactory("admin", "password", Map.of()))
                .build());
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) fail("Timed out waiting for: " + what);
            Thread.sleep(20);
        }
    }

    private StatementContext<?> onlyCursor() throws InterruptedException {
        await(() -> producer.statementLoadingCache.size() == 1, "the query to start");
        return producer.statementLoadingCache.asMap().values().iterator().next();
    }

    @Test
    void aFlightClientThatStopsReadingStallsTheServer() throws Exception {
        FlightStream stream = client.getStream(client.execute(ENDLESS_STREAM).getEndpoints().get(0).getTicket());
        try {
            assertTrue(stream.next());
            var cursor = onlyCursor();
            // The client now reads nothing. Once the transport's buffers are full the server must wait:
            // without backpressure it would keep producing (gigabytes within seconds) into direct memory.
            Thread.sleep(1_000);
            long afterOneSecond = cursor.bytesOut();
            Thread.sleep(2_000);
            long afterThreeSeconds = cursor.bytesOut();
            assertEquals(afterOneSecond, afterThreeSeconds,
                    "the server kept producing for a client that is not reading");
            assertTrue(stream.next(), "the stream resumes when the client reads again");
        } finally {
            try {
                stream.cancel("done", null);
                stream.close();
            } catch (Exception ignored) {
                // a cancelled stream may report the cancellation on close
            }
        }
        await(() -> producer.statementLoadingCache.size() == 0, "the query to stop");
    }

    private static final long DISCONNECT_AFTER_BYTES = 16 * 1024;

    /** An HTTP response body whose client disconnects after a few kilobytes; counts what it was sent. */
    private static Supplier<OutputStream> disconnectingClient(AtomicLong written) {
        return () -> new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                write(new byte[]{(byte) b}, 0, 1);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                if (written.addAndGet(len) > DISCONNECT_AFTER_BYTES) throw new IOException("Broken pipe");
            }
        };
    }

    private static FlightSql.TicketStatementQuery ticket(String sql) throws Exception {
        var handle = StatementHandle.newStatementHandle(sql, "p", -1);
        return FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFrom(handle.serialize()))
                .build();
    }

    private void assertHttpDisconnectStopsTheQuery(
            BiFunction<FlightSql.TicketStatementQuery, Supplier<OutputStream>, CompletableFuture<Void>> stream)
            throws Exception {
        var written = new AtomicLong();
        CompletableFuture<Void> response = stream.apply(ticket(ENDLESS_STREAM), disconnectingClient(written));
        await(response::isDone, "the response to fail");
        assertTrue(response.isCompletedExceptionally());
        // The query really streamed, and failed because the client went away, not for another reason.
        assertTrue(written.get() > DISCONNECT_AFTER_BYTES, "no rows were streamed before the failure: " + written);
        await(() -> producer.statementLoadingCache.size() == 0, "the query to stop after its client went away");
    }

    @Test
    void anHttpArrowResponseCompletesNormally() throws Exception {
        var body = new java.io.ByteArrayOutputStream();
        producer.getStreamStatementDirect(ticket("SELECT * FROM range(100000)"), admin, () -> body)
                .get(30, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(body.size() > 0);
    }

    private final FlightProducer.CallContext admin =
            new SyntheticFlightContext(Map.of(), new SubjectAndVerifiedClaims("admin", Map.of()));

    @Test
    void anHttpJsonlClientThatDisconnectsStopsItsQuery() throws Exception {
        assertHttpDisconnectStopsTheQuery((t, out) -> producer.streamJsonl(t, admin, out));
    }

    @Test
    void anHttpArrowClientThatDisconnectsStopsItsQuery() throws Exception {
        assertHttpDisconnectStopsTheQuery((t, out) -> producer.getStreamStatementDirect(t, admin, out));
    }

    @Test
    void anHttpTsvClientThatDisconnectsStopsItsQuery() throws Exception {
        assertHttpDisconnectStopsTheQuery((t, out) -> producer.streamTsv(t, admin, out));
    }

    @Test
    void stalledClientsDoNotHoldTheThreadsOtherQueriesNeed() throws Exception {
        // More stalled streams than the DuckDB platform pool has threads. Each client reads one batch
        // of an endless query and stops. With stream tasks on that fixed pool, every thread would be
        // parked in the backpressure wait and the query below would queue forever; on virtual
        // threads a stalled stream holds no platform thread.
        int stalled = Runtime.getRuntime().availableProcessors() + 2;
        var streams = new java.util.ArrayList<FlightStream>();
        try {
            for (int i = 0; i < stalled; i++) {
                FlightStream stream = client.getStream(client.execute(ENDLESS_STREAM).getEndpoints().get(0).getTicket());
                assertTrue(stream.next(), "stalled client " + i + " got its first batch");
                streams.add(stream);
            }
            var answer = CompletableFuture.supplyAsync(() -> {
                try (FlightStream s = client.getStream(client.execute("SELECT 42 AS answer").getEndpoints().get(0).getTicket())) {
                    assertTrue(s.next());
                    return ((org.apache.arrow.vector.IntVector) s.getRoot().getVector(0)).get(0);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            assertEquals(42, answer.get(20, java.util.concurrent.TimeUnit.SECONDS),
                    "a new query must run while " + stalled + " clients are stalled");
        } finally {
            for (FlightStream stream : streams) {
                try {
                    stream.cancel("done", null);
                    stream.close();
                } catch (Exception ignored) {
                    // a cancelled stream may report the cancellation on close
                }
            }
        }
        await(() -> producer.statementLoadingCache.size() == 0, "the stalled queries to stop");
    }

    @Test
    void streamsWaitOnVirtualThreadsAndCallDuckDbOnPlatformThreads() throws Exception {
        var streamThreadVirtual = new java.util.concurrent.atomic.AtomicReference<Boolean>();
        var duckdbThreadVirtual = new java.util.concurrent.atomic.AtomicReference<Boolean>();
        var connection = io.dazzleduck.sql.commons.ConnectionPool.getConnection();
        var ctx = new StatementContext<>(connection, connection.createStatement(), "SELECT 1");
        var done = new java.util.concurrent.CountDownLatch(1);
        var listener = new FlightProducer.ServerStreamListener() {
            @Override public boolean isCancelled() { return false; }
            @Override public void setOnCancelHandler(Runnable handler) { }
            @Override public boolean isReady() { return true; }
            @Override public void start(org.apache.arrow.vector.VectorSchemaRoot root,
                                        org.apache.arrow.vector.dictionary.DictionaryProvider dictionaries,
                                        org.apache.arrow.vector.ipc.message.IpcOption option) {
                streamThreadVirtual.set(Thread.currentThread().isVirtual());
            }
            @Override public void putNext() { }
            @Override public void putNext(org.apache.arrow.memory.ArrowBuf metadata) { }
            @Override public void putMetadata(org.apache.arrow.memory.ArrowBuf metadata) { }
            @Override public void error(Throwable ex) { done.countDown(); }
            @Override public void completed() { done.countDown(); }
        };
        ResultSetStreamUtil.streamResultSet(producer.streamExecutors, ctx,
                new DuckDBFlightSqlProducer.CacheKey("admin", StatementHandle.nextStatementId()),
                new OptionalResultSetSupplier() {
                    @Override public boolean hasResultSet() { return false; }
                    @Override public org.duckdb.DuckDBResultSet get() { return null; }
                    @Override public void execute() { duckdbThreadVirtual.set(Thread.currentThread().isVirtual()); }
                }, allocator, 1024, listener, ctx::close,
                new io.dazzleduck.sql.flight.MicroMeterFlightRecorder(
                        new io.micrometer.core.instrument.simple.SimpleMeterRegistry(), "test"));
        assertTrue(done.await(10, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(Boolean.TRUE, streamThreadVirtual.get(), "the stream task runs on a virtual thread");
        assertEquals(Boolean.FALSE, duckdbThreadVirtual.get(),
                "DuckDB calls run on a platform thread (a native call would pin a virtual thread's carrier)");
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

    /**
     * A stream waiting for a stalled client that the server, not the client, ends must end with an
     * error status: completing it would tell the client a cut-off result was the whole result.
     */
    private static void assertEndedWith(StalledListener listener, CallStatus status) throws InterruptedException {
        assertTrue(listener.done.await(10, TimeUnit.SECONDS), "the stream must end");
        assertFalse(listener.completed, "a cut-off result must not be reported as complete");
        var error = assertInstanceOf(FlightRuntimeException.class, listener.error);
        assertEquals(status.code(), error.status().code(), String.valueOf(error));
    }

    /** Streams a never-ending statement query to a stalled client on {@code streams}. */
    private StatementContext<?> streamToAStalledClient(ExecutorService streams, StalledListener listener,
                                                       CountDownLatch cleanedUp) throws Exception {
        var connection = ConnectionPool.getConnection();
        var ctx = new StatementContext<>(connection, connection.createStatement(), ENDLESS_STREAM);
        ctx.markClaimed();
        ResultSetStreamUtil.streamResultSet(StreamExecutors.sameThread(streams), ctx,
                new DuckDBFlightSqlProducer.CacheKey("admin", StatementHandle.nextStatementId()),
                OptionalResultSetSupplier.of(ctx.getStatement(), ENDLESS_STREAM),
                allocator, 1024, listener, () -> { ctx.close(); cleanedUp.countDown(); },
                new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test"));
        assertTrue(listener.waiting.await(10, TimeUnit.SECONDS), "the stream waits for its client");
        return ctx;
    }

    @Test
    void aStatementStreamInterruptedByShutdownEndsWithAnError() throws Exception {
        ExecutorService streams = Executors.newSingleThreadExecutor();
        var listener = new StalledListener();
        var cleanedUp = new CountDownLatch(1);
        streamToAStalledClient(streams, listener, cleanedUp);

        streams.shutdownNow(); // close()'s fallback, without a cancel first

        assertEndedWith(listener, CallStatus.UNAVAILABLE);
        assertTrue(cleanedUp.await(10, TimeUnit.SECONDS), "the stream gives back its connection");
    }

    @Test
    void aMetadataStreamInterruptedByShutdownEndsWithAnError() throws Exception {
        ExecutorService streams = Executors.newSingleThreadExecutor();
        var connection = ConnectionPool.getConnection();
        var listener = new StalledListener();
        ResultSetStreamUtil.streamResultSet(StreamExecutors.sameThread(streams),
                () -> (DuckDBResultSet) connection.createStatement().executeQuery(ENDLESS_STREAM),
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
        ExecutorService streams = Executors.newSingleThreadExecutor();
        try {
            var listener = new StalledListener();
            var cleanedUp = new CountDownLatch(1);
            var ctx = streamToAStalledClient(streams, listener, cleanedUp);

            assertEquals(StatementContext.CancelOutcome.INTERRUPTED, ctx.cancel()); // like CancelFlightInfo

            // Without the client reading again: the stream notices on its next recheck and ends,
            // giving back its connection.
            assertEndedWith(listener, CallStatus.CANCELLED);
            assertTrue(cleanedUp.await(10, TimeUnit.SECONDS), "the stream gives back its connection");
        } finally {
            streams.shutdownNow();
        }
    }
}
