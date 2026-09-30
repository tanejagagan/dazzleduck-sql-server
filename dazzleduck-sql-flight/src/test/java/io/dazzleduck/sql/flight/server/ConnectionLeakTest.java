package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/** Connections opened for a request are released on every failure path. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(60)
class ConnectionLeakTest {

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
                null, Executors.newSingleThreadScheduledExecutor(), Duration.ofMinutes(1), Duration.ZERO,
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

    @Test
    void aPrepareThatFailsAfterPreparingLeavesNothingCached() {
        // HUGEINT results cannot be mapped by the Arrow schema conversion, which runs after DuckDB has
        // prepared the statement. The client gets an error and no handle, so nothing may stay cached
        // (holding its connection) that it could never close.
        long before = producer.preparedStatementLoadingCache.size();
        assertThrows(FlightRuntimeException.class, () -> client.prepare("SELECT 1::HUGEINT AS h").close());
        assertEquals(before, producer.preparedStatementLoadingCache.size());
    }

    @Test
    void aSuccessfulPrepareIsCachedUntilClosed() throws Exception {
        long before = producer.preparedStatementLoadingCache.size();
        try (var prepared = client.prepare("SELECT 1 AS i")) {
            assertEquals(before + 1, producer.preparedStatementLoadingCache.size());
        }
        assertEquals(before, producer.preparedStatementLoadingCache.size());
    }

    @Test
    void aStreamRejectedAtShutdownStillRunsItsCleanup() {
        var executor = Executors.newSingleThreadExecutor();
        executor.shutdown();
        var cleanedUp = new AtomicBoolean();
        assertThrows(RejectedExecutionException.class, () -> ResultSetStreamUtil.streamResultSet(
                executor, () -> { throw new AssertionError("must not run"); }, allocator, 1024,
                null, () -> cleanedUp.set(true),
                new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test")));
        assertTrue(cleanedUp.get(), "the stream's cleanup (which closes its connection) did not run");
    }
}
