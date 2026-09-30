package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The cursor TTL reaps cursors nobody is using, but must not close a query that is still executing
 * or streaming: previously the cache's removal listener closed the statement and connection of any
 * expired entry, so a query running longer than the TTL failed as soon as another query triggered
 * the cache's cleanUp().
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CursorTtlTest {

    private static final long TTL_MS = 300;
    // About 2 s on a laptop: comfortably longer than the TTL.
    private static final String SLOW_QUERY = "SELECT sum(a.range * b.range)::BIGINT AS s FROM range(30000) a, range(30000) b";

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
                null, Executors.newSingleThreadScheduledExecutor(), Duration.ofMinutes(2), Duration.ZERO,
                Clock.systemDefaultZone(), new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test"),
                DuckDBFlightSqlProducer.DEFAULT_INGESTION_CONFIG, List.of(),
                new CursorConfig(TTL_MS, 50, 2000));
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

    private long firstLong(String sql) throws Exception {
        FlightInfo info = client.execute(sql);
        try (FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket())) {
            assertTrue(stream.next(), "expected a batch");
            FieldVector vector = stream.getRoot().getVector(0);
            return ((BigIntVector) vector).get(0);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aQueryRunningPastTheTtlIsNotClosedByAnotherQuerysCleanup() throws Exception {
        long expected = ConnectionPool.collectFirst(SLOW_QUERY, Long.class);
        CompletableFuture<Long> slow = CompletableFuture.supplyAsync(() -> {
            try {
                return firstLong(SLOW_QUERY);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        // Let the slow query's cursor outlive the TTL, then make the cache clean up by starting
        // another query (enforceCursorLimits reaps idle cursors).
        Thread.sleep(TTL_MS * 3);
        assertFalse(slow.isDone(), "the slow query must still be running for this test to mean anything");
        assertEquals(1L, firstLong("SELECT 1::BIGINT"));

        assertEquals(expected, slow.get(50, TimeUnit.SECONDS), "the slow query finished with its result");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anIdleCursorIsStillReapedAndClosedAfterTheTtl() throws Exception {
        producer.injectTestCursor("idle-user");
        var context = producer.statementLoadingCache.asMap().values().stream()
                .filter(c -> !c.running()).findFirst().orElseThrow();
        Thread.sleep(TTL_MS * 3);
        producer.reapIdleCursors();

        assertFalse(producer.statementLoadingCache.asMap().containsValue(context), "evicted after the TTL");
        assertTrue(context.getStatement().isClosed(), "an idle cursor is closed on eviction, not deferred");
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aQueryRunningPastTheTtlStaysCancellableAndCounted() throws Exception {
        FlightInfo info = client.execute(SLOW_QUERY);
        CompletableFuture<Long> slow = CompletableFuture.supplyAsync(() -> {
            try (FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket())) {
                assertTrue(stream.next(), "expected a batch");
                return ((BigIntVector) stream.getRoot().getVector(0)).get(0);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(TTL_MS * 3);
        assertEquals(1L, firstLong("SELECT 1::BIGINT")); // reaps idle cursors
        assertFalse(slow.isDone(), "the slow query must still be running for this test to mean anything");
        assertTrue(producer.statementLoadingCache.asMap().values().stream().anyMatch(StatementContext::running),
                "a running query stays in the cache (findable by cancel, counted by the limits)");

        client.cancelFlightInfo(new CancelFlightInfoRequest(info));
        var ex = assertThrows(java.util.concurrent.ExecutionException.class, () -> slow.get(30, TimeUnit.SECONDS));
        assertNotNull(ex.getCause(), "the cancelled query fails instead of running to the end");
    }

    @Test
    void aClaimedCursorIsNotIdleWhileQueuedButIsAfterItsStreamEnds() throws Exception {
        try (var connection = ConnectionPool.getConnection()) {
            var ctx = new StatementContext<>(connection, connection.createStatement(), "SELECT 1");
            Duration ttl = Duration.ofMillis(TTL_MS);
            ctx.markClaimed(); // handed to a stream task still waiting for an executor thread
            Thread.sleep(TTL_MS * 2);
            assertFalse(ctx.idleLongerThan(ttl), "a queued query is never idle");
            ctx.start();
            Thread.sleep(TTL_MS * 2);
            assertFalse(ctx.idleLongerThan(ttl), "a running query is never idle");
            ctx.end();
            assertFalse(ctx.idleLongerThan(ttl), "idle time counts from the end of the stream");
            Thread.sleep(TTL_MS * 2);
            assertTrue(ctx.idleLongerThan(ttl));
        }
    }
}
