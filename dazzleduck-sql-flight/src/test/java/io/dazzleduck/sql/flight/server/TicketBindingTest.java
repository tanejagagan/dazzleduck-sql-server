package io.dazzleduck.sql.flight.server;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.util.MutableClock;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Signed tickets skip authorization, so they must only work for the user they were issued to and
 * only until they expire.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TicketBindingTest {

    private static final String SECRET = "test-secret";

    private final MutableClock clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
    private BufferAllocator allocator;
    private FlightServer server;
    private FlightSqlClient alice;
    private FlightSqlClient bob;

    @BeforeAll
    void setup() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        ConnectionPool.executeBatch(new String[]{"INSTALL arrow FROM community", "LOAD arrow"});
        Location location = FlightTestUtils.findNextLocation();
        var producer = new DuckDBFlightSqlProducer(
                location,
                UUID.randomUUID().toString(),
                SECRET,
                allocator,
                System.getProperty("java.io.tmpdir"),
                AccessMode.COMPLETE,
                DuckDBFlightSqlProducer.newTempDir(),
                null,
                Executors.newSingleThreadScheduledExecutor(),
                Duration.ofMinutes(2),
                Duration.ZERO,
                clock,
                new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test"),
                DuckDBFlightSqlProducer.DEFAULT_INGESTION_CONFIG,
                List.of());
        server = FlightServer.builder(allocator, location, producer)
                .headerAuthenticator(AuthUtils.getTestAuthenticator())
                .build()
                .start();
        alice = client(location, "alice");
        bob = client(location, "bob");
    }

    private FlightSqlClient client(Location location, String user) {
        return new FlightSqlClient(FlightClient.builder(allocator, location)
                .intercept(AuthUtils.createClientMiddlewareFactory(user, "password", Map.of()))
                .build());
    }

    @AfterAll
    void teardown() throws Exception {
        if (alice != null) alice.close();
        if (bob != null) bob.close();
        if (server != null) server.close();
        if (allocator != null) allocator.close();
    }

    private static Ticket ticketOf(FlightInfo info) {
        return info.getEndpoints().get(0).getTicket();
    }

    private static long rows(FlightSqlClient client, Ticket ticket) throws Exception {
        long rows = 0;
        try (FlightStream stream = client.getStream(ticket)) {
            while (stream.next()) rows += stream.getRoot().getRowCount();
        }
        return rows;
    }

    private static void assertRejected(FlightSqlClient client, Ticket ticket) {
        var ex = assertThrows(FlightRuntimeException.class, () -> rows(client, ticket));
        assertEquals(FlightStatusCode.UNAUTHORIZED, ex.status().code(), ex.getMessage());
    }

    @Test
    void ticketWorksForTheUserItWasIssuedTo() throws Exception {
        assertEquals(3, rows(alice, ticketOf(alice.execute("SELECT * FROM range(3)"))));
    }

    @Test
    void ticketIsRejectedForAnotherUser() {
        assertRejected(bob, ticketOf(alice.execute("SELECT * FROM range(3)")));
    }

    @Test
    void ticketIsRejectedAfterItExpires() {
        Ticket ticket = ticketOf(alice.execute("SELECT * FROM range(3)"));
        clock.advanceBy(DuckDBFlightSqlProducer.TICKET_TTL.plusSeconds(1));
        assertRejected(alice, ticket);
    }

    @Test
    void rewritingTheBoundFieldsBreaksTheSignature() throws Exception {
        Ticket ticket = ticketOf(alice.execute("SELECT * FROM range(3)"));
        var any = FlightSqlUtils.parseOrThrow(ticket.getBytes());
        var h = StatementHandle.deserialize(
                FlightSqlUtils.unpackOrThrow(any, FlightSql.TicketStatementQuery.class).getStatementHandle());
        var forged = new StatementHandle(h.query(), h.queryId(), h.producerId(), h.splitSize(),
                h.queryChecksum(), "bob", 0);
        var forgedTicket = new Ticket(Any.pack(FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFrom(forged.serialize())).build()).toByteArray());
        assertRejected(bob, forgedTicket);
    }

    @Test
    void preparedStatementHandleIsBoundToItsUserButDoesNotExpire() {
        var handle = StatementHandle.newStatementHandle("SELECT 1", "p", -1).signed(SECRET, "alice", 0);
        long now = clock.millis();
        assertTrue(handle.validFor(SECRET, "alice", now + Duration.ofDays(365).toMillis()));
        assertFalse(handle.validFor(SECRET, "bob", now));
        assertFalse(handle.validFor("other-secret", "alice", now));
    }

    @Test
    void principalCannotBeShiftedIntoTheQueryOrExpiry() {
        // Principal "a", expiry 1, query "0:x" must not share a signature with principal "a:1",
        // expiry 0, query "x", or an expired ticket could be replayed as a non-expiring one.
        var handle = new StatementHandle("0:x", 5, "p", -1).signed(SECRET, "a", 1);
        var shifted = new StatementHandle("x", 5, "p", -1, handle.queryChecksum(), "a:1", 0);
        assertFalse(shifted.validFor(SECRET, "a:1", 0));
    }
}
