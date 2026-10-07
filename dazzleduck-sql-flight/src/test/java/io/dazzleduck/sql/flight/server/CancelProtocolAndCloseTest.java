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
        Path scratch = producer.scratchDir();
        producer.close();
        assertTrue(ctx.getStatement().isClosed(), "close() left a prepared statement and its connection open");
        assertFalse(Files.exists(scratch), "close() left its scratch directory behind");
        // The location is shared with other servers and collectors; it must outlive this producer.
        assertTrue(Files.exists(tempDir), "close() deleted the shared temp_write_location");
    }

    @Test
    void closeLeavesOtherProcessesStagedFilesAlone() throws Exception {
        // What another server, or an OTel collector, staging into the same temp_write_location has
        // in flight when this one shuts down. Deleting it fails their ingest with "No files found".
        Path otherServersBatch = Files.writeString(tempDir.resolve("ingestion_other-server.arrow"), "x");
        Path collectorScratch = Files.createDirectory(tempDir.resolve("otel-logs-4711"));
        Path collectorsBatch = Files.writeString(collectorScratch.resolve("batch.arrow"), "y");

        Path scratch = producer.scratchDir();
        assertTrue(scratch.startsWith(tempDir) && !scratch.equals(tempDir),
                "the producer must stage in a private child of the location, not the location itself");

        producer.close();

        assertFalse(Files.exists(scratch), "close() left its scratch directory behind");
        assertTrue(Files.exists(otherServersBatch), "close() deleted another server's staged batch");
        assertTrue(Files.exists(collectorsBatch), "close() deleted an OTel collector's staged batch");
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
            assertFalse(Files.exists(producer.scratchDir()), "scratch dir was not cleaned up");
            assertTrue(Files.exists(tempDir), "close() deleted the shared temp_write_location");
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

    @Test
    void aLateCancelOfAFinishedPreparedRunKeepsThePreparedStatement() throws Exception {
        try (var prepared = client.prepare("SELECT 1")) {
            FlightInfo info = prepared.execute();
            try (FlightStream st = client.getStream(info.getEndpoints().get(0).getTicket())) {
                while (st.next()) { }
            }
            await(() -> producer.preparedStatementLoadingCache.asMap().values().stream()
                    .noneMatch(StatementContext::running), "the run to end");
            var ex = assertThrows(FlightRuntimeException.class,
                    () -> client.cancelFlightInfo(new CancelFlightInfoRequest(info), TIMEOUT));
            assertEquals(FlightStatusCode.NOT_FOUND, ex.status().code(), "nothing was running to cancel");
            assertEquals(1, producer.preparedStatementLoadingCache.size(), "the prepared statement stays");
            try (FlightStream st = client.getStream(prepared.execute().getEndpoints().get(0).getTicket())) {
                assertTrue(st.next(), "and runs again");
            }
        }
    }

    @Test
    void cancellingAQueuedPreparedRunKeepsThePreparedStatement() throws Exception {
        var connection = io.dazzleduck.sql.commons.ConnectionPool.getConnection();
        var statement = connection.prepareStatement("SELECT 1");
        var ctx = new StatementContext<>(connection, statement, "SELECT 1", true);
        var key = new DuckDBFlightSqlProducer.CacheKey("admin", StatementHandle.nextStatementId());
        producer.preparedStatementLoadingCache.put(key, ctx);
        ctx.markClaimed(); // a run is queued, not yet executing
        var caller = new io.dazzleduck.sql.flight.context.SyntheticFlightContext(Map.of(),
                new io.dazzleduck.sql.commons.authorization.SubjectAndVerifiedClaims("admin", Map.of()));
        assertTrue(producer.tryCancel(key.id(), caller), "the queued run was cancelled");
        assertTrue(ctx.isCancelRequested());
        assertSame(ctx, producer.preparedStatementLoadingCache.getIfPresent(key),
                "it never executed, so DuckDB did not close it: keep it");
        assertFalse(statement.isClosed());
    }

    @Test
    void closeClosesAContextWhoseQueuedStreamNeverRan() throws Exception {
        var connection = io.dazzleduck.sql.commons.ConnectionPool.getConnection();
        var statement = connection.createStatement();
        var ctx = new StatementContext<>(connection, statement, "SELECT 1");
        ctx.markClaimed(); // its stream task was queued, and shutdown drops it before it runs
        producer.statementLoadingCache.put(new DuckDBFlightSqlProducer.CacheKey("admin",
                StatementHandle.nextStatementId()), ctx);
        producer.close();
        assertTrue(statement.isClosed(), "a claimed context must not outlive the producer");
    }
}
