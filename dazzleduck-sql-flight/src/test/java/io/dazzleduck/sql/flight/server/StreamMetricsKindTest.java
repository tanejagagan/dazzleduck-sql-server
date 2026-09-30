package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
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
 * Plain statement streams and prepared-statement streams are counted under their own metrics
 * (#472: DuckDB's createStatement() also returns a PreparedStatement, which counted every stream as
 * a prepared one).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(60)
class StreamMetricsKindTest {

    private BufferAllocator allocator;
    private FlightServer server;
    private FlightSqlClient client;
    private MicroMeterFlightRecorder recorder;

    @BeforeAll
    void setup() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        Location location = FlightTestUtils.findNextLocation();
        recorder = new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test");
        var producer = new DuckDBFlightSqlProducer(
                location, UUID.randomUUID().toString(), "test-secret", allocator,
                System.getProperty("java.io.tmpdir"), AccessMode.COMPLETE, DuckDBFlightSqlProducer.newTempDir(),
                null, Executors.newSingleThreadScheduledExecutor(), Duration.ofMinutes(1), Duration.ZERO,
                Clock.systemDefaultZone(), recorder, DuckDBFlightSqlProducer.DEFAULT_INGESTION_CONFIG, List.of());
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

    private void drain(Ticket ticket) throws Exception {
        try (FlightStream stream = client.getStream(ticket)) {
            while (stream.next()) { }
        }
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) fail("Timed out waiting for: " + what);
            Thread.sleep(20);
        }
    }

    @Test
    void aPlainStatementStreamIsCountedAsAStatement() throws Exception {
        long statements = recorder.getCompletedStatements();
        long prepared = recorder.getCompletedPreparedStatements();
        drain(client.execute("SELECT 1").getEndpoints().get(0).getTicket());
        await(() -> recorder.getCompletedStatements() == statements + 1, "the statement stream to be counted");
        assertEquals(prepared, recorder.getCompletedPreparedStatements(), "not as a prepared statement");
    }

    @Test
    void aPreparedStatementStreamIsCountedAsPrepared() throws Exception {
        long statements = recorder.getCompletedStatements();
        long prepared = recorder.getCompletedPreparedStatements();
        try (var ps = client.prepare("SELECT 1")) {
            drain(ps.execute().getEndpoints().get(0).getTicket());
        }
        await(() -> recorder.getCompletedPreparedStatements() == prepared + 1, "the prepared stream to be counted");
        assertEquals(statements, recorder.getCompletedStatements(), "not as a plain statement");
    }
}
