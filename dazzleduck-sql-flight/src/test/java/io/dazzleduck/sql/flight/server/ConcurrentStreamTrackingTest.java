package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A running stream must stay tracked (cancellable, counted against cursor limits, not closed under
 * it) when a second call tries to run the same ticket or the same prepared statement concurrently.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(60)
class ConcurrentStreamTrackingTest {

    private static final String ENDLESS_STREAM = "SELECT * FROM range(10000000000)";
    private static final String SLOW_TO_EXECUTE = "SELECT sum(a.range * b.range)::BIGINT FROM range(1000000) a, range(1000000) b";

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
        if (allocator != null) allocator.close();
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) fail("Timed out waiting for: " + what);
            Thread.sleep(20);
        }
    }

    private static void cancelQuietly(FlightStream stream) {
        try {
            stream.cancel("done", null);
            stream.close();
        } catch (Exception ignored) {
            // a cancelled stream may report the cancellation on close
        }
    }

    private static long rows(FlightStream stream) throws Exception {
        long rows = 0;
        try (stream) {
            while (stream.next()) rows += stream.getRoot().getRowCount();
        }
        return rows;
    }

    private static FlightStatusCode statusOf(FlightStream stream) {
        var ex = assertThrows(FlightRuntimeException.class, () -> rows(stream));
        return ex.status().code();
    }

    @Test
    void secondConcurrentStreamOfATicketIsRejectedAndTheFirstStaysTracked() throws Exception {
        Ticket ticket = client.execute(ENDLESS_STREAM).getEndpoints().get(0).getTicket();
        FlightStream first = client.getStream(ticket);
        try {
            assertTrue(first.next());
            assertEquals(FlightStatusCode.ALREADY_EXISTS, statusOf(client.getStream(ticket)));

            // The first stream is still the tracked cursor, and still streaming.
            assertEquals(1, producer.statementLoadingCache.size());
            assertTrue(producer.statementLoadingCache.asMap().values().iterator().next().running());
            assertTrue(first.next());
        } finally {
            cancelQuietly(first);
        }
        await(() -> producer.statementLoadingCache.size() == 0, "first stream to be removed");
    }

    @Test
    void aTicketCanBeStreamedAgainAfterTheFirstStreamEnds() throws Exception {
        Ticket ticket = client.execute("SELECT * FROM range(10)").getEndpoints().get(0).getTicket();
        assertEquals(10, rows(client.getStream(ticket)));
        await(() -> producer.statementLoadingCache.size() == 0, "first stream to be removed");
        assertEquals(10, rows(client.getStream(ticket)));
    }

    @Test
    void secondConcurrentRunOfAPreparedStatementIsRejectedAndTheFirstStaysInUse() throws Exception {
        try (var prepared = client.prepare(SLOW_TO_EXECUTE)) {
            FlightStream first = client.getStream(prepared.execute().getEndpoints().get(0).getTicket());
            try {
                await(() -> producer.preparedStatementLoadingCache.asMap().values().stream()
                        .anyMatch(StatementContext::running), "first run to start");
                var ctx = producer.preparedStatementLoadingCache.asMap().values().iterator().next();

                assertEquals(FlightStatusCode.ALREADY_EXISTS,
                        statusOf(client.getStream(prepared.execute().getEndpoints().get(0).getTicket())));
                assertTrue(ctx.running(), "a rejected run marked the running one as finished");
            } finally {
                cancelQuietly(first);
            }
        }
    }

    @Test
    void preparedStatementCanRunAgainAfterTheFirstRunEnds() throws Exception {
        try (var prepared = client.prepare("SELECT * FROM range(5)")) {
            assertEquals(5, rows(client.getStream(prepared.execute().getEndpoints().get(0).getTicket())));
            await(() -> producer.preparedStatementLoadingCache.asMap().values().stream()
                    .noneMatch(StatementContext::running), "first run to end");
            assertEquals(5, rows(client.getStream(prepared.execute().getEndpoints().get(0).getTicket())));
        }
    }

    @Test
    void aRejectedPreparedRunDoesNotChangeTheRunningStatementsTimeout() throws Exception {
        try (var prepared = client.prepare("SELECT 1")) {
            var ctx = producer.preparedStatementLoadingCache.asMap().values().stream()
                    .filter(c -> "SELECT 1".equals(c.getQuery())).findFirst().orElseThrow();
            ctx.getStatement().setQueryTimeout(111);
            assertTrue(ctx.tryStart()); // a run is executing on the shared statement
            try {
                var headers = new FlightCallHeaders();
                headers.insert(io.dazzleduck.sql.common.Headers.HEADER_QUERY_TIMEOUT, "7");
                var ticket = prepared.execute().getEndpoints().get(0).getTicket();
                var ex = assertThrows(FlightRuntimeException.class,
                        () -> rows(client.getStream(ticket, new HeaderCallOption(headers))));
                assertEquals(FlightStatusCode.ALREADY_EXISTS, ex.status().code());
                assertEquals(111, ctx.getStatement().getQueryTimeout(),
                        "the rejected run must not change the running statement's timeout");
            } finally {
                ctx.end();
            }
        }
    }

    @Test
    void aClosedPreparedStatementIsNotFoundNotAlreadyRunning() throws Exception {
        var connection = io.dazzleduck.sql.commons.ConnectionPool.getConnection();
        var ctx = new StatementContext<>(connection, connection.prepareStatement("SELECT 1"), "SELECT 1", true);
        ctx.close(); // e.g. a concurrent closePreparedStatement won the race
        var error = ErrorHandling.cannotStart(ctx);
        assertEquals(FlightStatusCode.NOT_FOUND, error.status().code(), error.getMessage());
        assertFalse(ctx.tryStart());
    }

    @Test
    void queryIdsStartAtARandomPointBelowTwoToThe53() {
        long id = StatementHandle.nextStatementId();
        assertTrue(id >= StatementHandle.QUERY_ID_MIN, "not a small id another node or an HTTP client would also use: " + id);
        assertTrue(id < StatementHandle.QUERY_ID_BOUND, "exact in JSON (JavaScript numbers): " + id);
        assertTrue(StatementHandle.nextStatementId() > id, "still increasing");
    }

    @Test
    void aRejectedStreamStillRunsItsOwnCleanup() throws Exception {
        var connection = io.dazzleduck.sql.commons.ConnectionPool.getConnection();
        var ctx = new StatementContext<>(connection, connection.createStatement(), "SELECT 1");
        ctx.close(); // a plain context can only fail tryStart() once closed
        var cleanedUp = new java.util.concurrent.CountDownLatch(1);
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            ResultSetStreamUtil.streamResultSet(executor, ctx, new DuckDBFlightSqlProducer.CacheKey("admin", 1L),
                    null, allocator, 1024, new NoOpListener(), cleanedUp::countDown,
                    new io.dazzleduck.sql.flight.MicroMeterFlightRecorder(
                            new io.micrometer.core.instrument.simple.SimpleMeterRegistry(), "test"));
            assertTrue(cleanedUp.await(10, java.util.concurrent.TimeUnit.SECONDS),
                    "the rejected stream's cleanup (removing its closed cursor entry) must run");
        } finally {
            executor.shutdown();
        }
    }

    private static final class NoOpListener implements FlightProducer.ServerStreamListener {
        @Override public boolean isCancelled() { return false; }
        @Override public void setOnCancelHandler(Runnable handler) { }
        @Override public boolean isReady() { return true; }
        @Override public void start(org.apache.arrow.vector.VectorSchemaRoot root,
                                    org.apache.arrow.vector.dictionary.DictionaryProvider dictionaries,
                                    org.apache.arrow.vector.ipc.message.IpcOption option) { }
        @Override public void putNext() { }
        @Override public void putNext(org.apache.arrow.memory.ArrowBuf metadata) { }
        @Override public void putMetadata(org.apache.arrow.memory.ArrowBuf metadata) { }
        @Override public void error(Throwable ex) { }
        @Override public void completed() { }
    }
}
