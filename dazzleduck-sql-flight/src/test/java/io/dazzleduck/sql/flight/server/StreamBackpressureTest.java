package io.dazzleduck.sql.flight.server;

import com.google.protobuf.ByteString;
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
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;
import java.util.function.BiFunction;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The server must not produce results faster than the client takes them, and must stop a query
 * whose HTTP client went away.
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
}
