package io.dazzleduck.sql.flight.server;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessType;
import io.dazzleduck.sql.commons.ingestion.NOOPIngestionTaskFactoryProvider;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.optimizer.QueryOptimizer;
import io.dazzleduck.sql.flight.server.auth2.AdvanceJWTTokenAuthenticator;
import io.dazzleduck.sql.flight.server.auth2.AdvanceServerCallHeaderAuthMiddleware;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.dazzleduck.sql.flight.stream.ArrowStreamReaderWrapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Flight SQL bulk ingestion authorization.
 *
 * Verifies that the JwtClaimBasedAuthorizer enforces write access by
 * checking the ingestion_queue claim embedded in the JWT at login time.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FlightSqlBulkIngestionAuthorizationTest {

    private static final String USER = "admin";
    private static final String PASSWORD = "password";
    private static final String CATALOG = "memory";
    private static final String SCHEMA = "main";
    private static final String INGESTION_QUEUE = "test_queue";

    private BufferAllocator allocator;
    private FlightServer server;
    private Location serverLocation;
    private String warehousePath;

    @BeforeAll
    void setup() throws Exception {
        allocator = new RootAllocator();
        warehousePath = Files.createTempDirectory("ingestion_auth_test").toString();
        serverLocation = FlightTestUtils.findNextLocation();

        String ingestionDir = warehousePath + File.separator + "ingestion";
        Files.createDirectories(Path.of(ingestionDir, INGESTION_QUEUE));

        String producerId = UUID.randomUUID().toString();
        var producer = new RestrictedFlightSqlProducer(
                serverLocation,
                producerId,
                "change me",
                allocator,
                warehousePath,
                DuckDBFlightSqlProducer.newTempDir(),
                new NOOPIngestionTaskFactoryProvider(ingestionDir).getIngestionTaskFactory(),
                Executors.newSingleThreadScheduledExecutor(),
                Duration.ofMinutes(2),
                Clock.systemDefaultZone(),
                new MicroMeterFlightRecorder(new SimpleMeterRegistry(), producerId),
                QueryOptimizer.NOOP_QUERY_OPTIMIZER,
                DuckDBFlightSqlProducer.DEFAULT_INGESTION_CONFIG
        );

        var authenticator = getIngestionTestAuthenticator();
        server = FlightServer.builder(allocator, serverLocation, producer)
                .middleware(AdvanceServerCallHeaderAuthMiddleware.KEY,
                        new AdvanceServerCallHeaderAuthMiddleware.Factory(authenticator))
                .build()
                .start();

        ConnectionPool.executeBatch(new String[]{
                "INSTALL arrow FROM community",
                "LOAD arrow"
        });
    }

    @AfterAll
    void teardown() throws Exception {
        if (server != null) server.close();
        if (allocator != null) allocator.close();
    }

    /**
     * Client with WRITE access_type and matching ingestion_queue claim should succeed.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testAuthorizedIngestionSucceeds() throws Exception {
        var client = clientWithWriteAccess(INGESTION_QUEUE);
        try {
            ingest(client, INGESTION_QUEUE);
            // no exception = success
        } finally {
            client.close();
        }
    }

    /**
     * Client with WRITE access but a different ingestion_queue claim should be rejected.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testIngestionRejectedForWrongQueue() throws Exception {
        var client = clientWithWriteAccess("other_queue");
        try {
            assertThrows(FlightRuntimeException.class, () -> ingest(client, INGESTION_QUEUE),
                    "Ingestion to a queue not in the JWT claim should be rejected");
        } finally {
            client.close();
        }
    }

    /**
     * Client with no access_type claim (read-only token) should be rejected.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testIngestionRejectedWithoutWriteClaim() throws Exception {
        var client = clientWithoutWriteAccess();
        try {
            assertThrows(FlightRuntimeException.class, () -> ingest(client, INGESTION_QUEUE),
                    "Ingestion without WRITE access_type claim should be rejected");
        } finally {
            client.close();
        }
    }

    // ==================== HELPERS ====================

    private FlightSqlClient clientWithWriteAccess(String queue) {
        return new FlightSqlClient(FlightClient.builder(allocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER, PASSWORD, Map.of(
                        Headers.HEADER_DATABASE, CATALOG,
                        Headers.HEADER_SCHEMA, SCHEMA,
                        Headers.HEADER_ACCESS_TYPE, AccessType.WRITE.name(),
                        Headers.QUERY_PARAMETER_INGESTION_QUEUE, queue
                )))
                .build());
    }

    private FlightSqlClient clientWithoutWriteAccess() {
        return new FlightSqlClient(FlightClient.builder(allocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER, PASSWORD, Map.of(
                        Headers.HEADER_DATABASE, CATALOG,
                        Headers.HEADER_SCHEMA, SCHEMA
                )))
                .build());
    }

    private static AdvanceJWTTokenAuthenticator getIngestionTestAuthenticator() throws Exception {
        var config = ConfigFactory.parseString("""
                jwt_token.claims.generate.headers=[database,schema,access-type,ingestion_queue]
                jwt_token.claims.validate.headers=[database,schema]
                """).withFallback(ConfigFactory.load().getConfig("dazzleduck_server"));
        return AuthUtils.getTestAuthenticator(config);
    }

    private void ingest(FlightSqlClient client, String queue) throws Exception {
        String query = "SELECT * FROM generate_series(5)";
        try (DuckDBConnection conn = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(conn, allocator, query, 1000)) {
            var streamReader = new ArrowStreamReaderWrapper(reader, allocator);
            var options = new FlightSqlClient.ExecuteIngestOptions(
                    "",
                    FlightSql.CommandStatementIngest.TableDefinitionOptions.newBuilder().build(),
                    false, "", "",
                    Map.of(Headers.QUERY_PARAMETER_INGESTION_QUEUE, queue));
            client.executeIngest(streamReader, options);
        }
    }
}
