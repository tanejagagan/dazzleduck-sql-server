package io.dazzleduck.sql.flight.server;

import com.google.protobuf.Any;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** CancelFlightInfo protocol handling, and producer close() releasing what it holds. */
@Timeout(60)
class CancelProtocolAndCloseTest {

    private static final String SLOW_TO_EXECUTE = "SELECT sum(a.range * b.range)::BIGINT FROM range(1000000) a, range(1000000) b";
    private static final CallOption TIMEOUT = CallOptions.timeout(10, TimeUnit.SECONDS);

    private BufferAllocator allocator;
    private Path tempDir;
    private FlightServer server;
    private FlightSqlClient client;
    private DuckDBFlightSqlProducer producer;

    @BeforeEach
    void setup() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        tempDir = DuckDBFlightSqlProducer.newTempDir();
        Location location = FlightTestUtils.findNextLocation();
        producer = new DuckDBFlightSqlProducer(
                location, UUID.randomUUID().toString(), "test-secret", allocator,
                System.getProperty("java.io.tmpdir"), AccessMode.COMPLETE, tempDir,
                null, Executors.newSingleThreadScheduledExecutor(), Duration.ofMinutes(10), Duration.ZERO,
                Clock.systemDefaultZone(), new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test"),
                DuckDBFlightSqlProducer.DEFAULT_INGESTION_CONFIG, List.of());
        producer.closeGrace = Duration.ofMillis(200);
        server = FlightServer.builder(allocator.newChildAllocator("server", 0, Long.MAX_VALUE), location, producer)
                .headerAuthenticator(AuthUtils.getTestAuthenticator())
                .build()
                .start();
        client = new FlightSqlClient(FlightClient.builder(new RootAllocator(), location)
                .intercept(AuthUtils.createClientMiddlewareFactory("admin", "password", Map.of()))
                .build());
    }

    @AfterEach
    void teardown() throws Exception {
        if (client != null) client.close();
        if (server != null) server.shutdown();
        producer.close(); // tests that closed it already exercise a second close() here
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) fail("Timed out waiting for: " + what);
            Thread.sleep(20);
        }
    }

    // Queries are held in their (long) execution phase rather than streaming rows: the DoGet loop
    // has no backpressure, so an endless stream read slowly would buffer gigabytes server-side.
    private long runningCursors() {
        return producer.statementLoadingCache.asMap().values().stream().filter(StatementContext::running).count();
    }

    private FlightStream startSlowQuery(FlightInfo info) throws InterruptedException {
        long before = runningCursors();
        FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket());
        await(() -> runningCursors() > before, "query to start");
        return stream;
    }

    private static void closeQuietly(FlightStream stream) {
        try {
            stream.cancel("done", null);
            stream.close();
        } catch (Exception ignored) {
            // a cancelled stream may report the cancellation on close
        }
    }

    private static FlightInfo infoWith(Ticket... tickets) {
        var endpoints = java.util.Arrays.stream(tickets).map(t -> new FlightEndpoint(t)).toArray(FlightEndpoint[]::new);
        return new FlightInfo(new Schema(List.of()), FlightDescriptor.command(new byte[0]), List.of(endpoints), -1, -1);
    }

    // ---- 11: CancelFlightInfo ----

    @Test
    void cancelReportsASingleCancelledStatus() throws Exception {
        FlightInfo info = client.execute(SLOW_TO_EXECUTE);
        FlightStream stream = startSlowQuery(info);
        try {
            CancelFlightInfoResult result = client.cancelFlightInfo(new CancelFlightInfoRequest(info), TIMEOUT);
            assertEquals(CancelStatus.CANCELLED, result.getStatus());
        } finally {
            closeQuietly(stream);
        }
    }

    @Test
    void cancelCoversEveryEndpoint() throws Exception {
        FlightInfo a = client.execute(SLOW_TO_EXECUTE);
        FlightInfo b = client.execute(SLOW_TO_EXECUTE);
        FlightStream sa = startSlowQuery(a);
        FlightStream sb = startSlowQuery(b);
        try {
            assertEquals(2, runningCursors());
            var both = infoWith(a.getEndpoints().get(0).getTicket(), b.getEndpoints().get(0).getTicket());
            assertEquals(CancelStatus.CANCELLED, client.cancelFlightInfo(new CancelFlightInfoRequest(both), TIMEOUT).getStatus());
            await(() -> producer.statementLoadingCache.size() == 0, "both queries to stop");
        } finally {
            closeQuietly(sa);
            closeQuietly(sb);
        }
    }

    @Test
    void unsupportedTicketTypeIsInvalidArgumentNotAHang() {
        var ticket = new Ticket(Any.pack(FlightSql.CommandGetCatalogs.getDefaultInstance()).toByteArray());
        var ex = assertThrows(FlightRuntimeException.class,
                () -> client.cancelFlightInfo(new CancelFlightInfoRequest(infoWith(ticket)), TIMEOUT));
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, ex.status().code(), ex.getMessage());
    }

    @Test
    void unreadableTicketIsInvalidArgument() {
        var ticket = new Ticket("not a ticket".getBytes(StandardCharsets.UTF_8));
        var ex = assertThrows(FlightRuntimeException.class,
                () -> client.cancelFlightInfo(new CancelFlightInfoRequest(infoWith(ticket)), TIMEOUT));
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, ex.status().code(), ex.getMessage());
    }

    @Test
    void cancellingAPreparedStatementRunStopsItAndLaterRunsGetNotFound() throws Exception {
        try (var prepared = client.prepare(SLOW_TO_EXECUTE)) {
            FlightInfo info = prepared.execute();
            FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket());
            try {
                await(() -> producer.preparedStatementLoadingCache.asMap().values().stream()
                        .anyMatch(StatementContext::running), "run to start");
                assertEquals(CancelStatus.CANCELLED, client.cancelFlightInfo(new CancelFlightInfoRequest(info), TIMEOUT).getStatus());
                await(() -> producer.preparedStatementLoadingCache.size() == 0, "run to stop");
            } finally {
                closeQuietly(stream);
            }
            // DuckDB closed the interrupted statement, so it is gone rather than half-usable.
            var ex = assertThrows(FlightRuntimeException.class,
                    () -> { try (var s = client.getStream(info.getEndpoints().get(0).getTicket())) { s.next(); } });
            assertEquals(FlightStatusCode.NOT_FOUND, ex.status().code(), ex.getMessage());
        } catch (FlightRuntimeException closeOfRemoved) {
            // closing a prepared statement the server already dropped may fail; not what is tested
        }
    }

    // ---- 13: close() ----

    @Test
    void closeReleasesIdlePreparedStatements() throws Exception {
        var prepared = client.prepare("SELECT 1");
        var ctx = producer.preparedStatementLoadingCache.asMap().values().iterator().next();
        producer.close();
        assertTrue(ctx.getStatement().isClosed(), "close() left a prepared statement and its connection open");
        assertFalse(Files.exists(tempDir));
    }

    @Test
    void closeStopsARunningStreamAndStillCleansUp() throws Exception {
        FlightStream stream = startSlowQuery(client.execute(SLOW_TO_EXECUTE));
        try {
            var ctx = producer.statementLoadingCache.asMap().values().iterator().next();
            long started = System.nanoTime();
            producer.close();
            assertTrue(Duration.ofNanos(System.nanoTime() - started).compareTo(Duration.ofSeconds(15)) < 0,
                    "close() did not stop the running query");
            await(() -> ctx.getStatement() != null && isClosed(ctx), "running statement to be closed");
            assertFalse(Files.exists(tempDir), "temp dir was not cleaned up");
        } finally {
            closeQuietly(stream);
        }
    }

    private static boolean isClosed(StatementContext<?> ctx) {
        try {
            return ctx.getStatement().isClosed();
        } catch (Exception e) {
            return true;
        }
    }
}
