package io.dazzleduck.sql.client;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.types.JavaRow;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.runtime.SharedTestServer;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.*;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GrpcFlightProducerTest {

    private static final String HOST = "localhost";
    private static final String USER = "admin";
    private static final String PASSWORD = "admin";
    private static final String CATALOG = "test_catalog";
    private static final String SCHEMA = "test_schema";

    private SharedTestServer server;
    private RootAllocator allocator;
    private FlightSqlClient client;
    private int flightPort;
    private String warehouse;

    @BeforeAll
    void setup() throws Exception {
        server = new SharedTestServer();
        server.start("ingestion.max_delay_ms=500");
        flightPort = server.getFlightPort();
        warehouse = server.getWarehousePath();

        allocator = new RootAllocator(Long.MAX_VALUE);

        ConnectionPool.executeBatch(new String[]{
                "ATTACH ':memory:' AS test_catalog",
                "CREATE SCHEMA IF NOT EXISTS test_catalog.test_schema"
        });

        client = new FlightSqlClient(
                FlightClient.builder(
                        allocator,
                        Location.forGrpcInsecure(HOST, flightPort))
                        .intercept(io.dazzleduck.sql.flight.server.auth2.AuthUtils
                        .createClientMiddlewareFactory(
                                        USER,
                                        PASSWORD,
                                        Map.of(
                                                Headers.HEADER_DATABASE, CATALOG,
                                                Headers.HEADER_SCHEMA, SCHEMA
                                        )
                                )
                ).build()
        );
    }

    @AfterAll
    void teardown() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception ignored) {
            }
        }
        if (allocator != null) {
            allocator.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void ingestAndVerifyValues() throws Exception {
        Schema schema = new Schema(List.of(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        String path =  "names";
        Files.createDirectories(Path.of(warehouse, path));

        try (GrpcFlightProducer sender = new GrpcFlightProducer(
                schema,
                1024,
                2048,
                Duration.ofMillis(200),
                Clock.systemUTC(),
                3,
                1000,
                java.util.List.of(),
                java.util.List.of(),
                5_000_000,
                20_000_000,
                allocator,
                Location.forGrpcInsecure(HOST, flightPort),
                USER,
                PASSWORD,
                CATALOG,
                SCHEMA,
                Map.of("path", path),
                Duration.ofSeconds(30)
        )) {
            sender.addRow(new JavaRow(new Object[]{"Aman"}));
            sender.addRow(new JavaRow(new Object[]{"Yash"}));
            sender.addRow(new JavaRow(new Object[]{"Sid"}));
        }

        var query = "SELECT name FROM read_parquet('%s/%s/*.parquet')".formatted(warehouse, path) + " ORDER BY name";

        FlightInfo flightInfo = client.execute(query);
        List<String> values = new java.util.ArrayList<>();

        try (FlightStream stream = client.getStream(flightInfo.getEndpoints().get(0).getTicket())) {

            while (stream.next()) {
                var root = stream.getRoot();
                var nameVector = (org.apache.arrow.vector.VarCharVector)
                        root.getVector("name");

                for (int i = 0; i < root.getRowCount(); i++) {
                    values.add(nameVector.getObject(i).toString());
                }
            }
        }
        assertEquals(List.of("Aman", "Sid", "Yash"), values);
    }





    @Test
    void testTransformationsAndPartitionByParameters() throws Exception {
        Schema schema = new Schema(List.of(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        String path = "both-params-grpc-test";
        Files.createDirectories(Path.of(warehouse, path));

        try (GrpcFlightProducer sender = new GrpcFlightProducer(
                schema,
                1024,
                2048,
                Duration.ofMillis(200),
                Clock.systemUTC(),
                3,
                1000,
                java.util.List.of("'c1' as c1", "'c2' as  c2"),
                java.util.List.of("c1", "c2"),
                5_000_000,
                20_000_000,
                allocator,
                Location.forGrpcInsecure(HOST, flightPort),
                USER,
                PASSWORD,
                CATALOG,
                SCHEMA,
                Map.of("path", path),
                Duration.ofSeconds(30)
        )) {
            sender.addRow(new JavaRow(new Object[]{1}));
            sender.addRow(new JavaRow(new Object[]{2}));
            sender.addRow(new JavaRow(new Object[]{3}));
        }

        var query = "SELECT count(*) as cnt FROM read_parquet('%s/%s/*/*/*.parquet')".formatted(warehouse, path);
        FlightInfo flightInfo = client.execute(query);

        try (FlightStream stream = client.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
            while (stream.next()) {
                var root = stream.getRoot();
                var countVector = (org.apache.arrow.vector.BigIntVector) root.getVector("cnt");
                assertEquals(3L, countVector.get(0));
            }
        }
    }



    @Test
    void testEmptyListsDoNotSendParameters() throws Exception {
        Schema schema = new Schema(List.of(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        String path = "empty-lists-grpc-test";
        Files.createDirectories(Path.of(warehouse, path));

        // Empty lists should work fine and not send parameters
        try (GrpcFlightProducer sender = new GrpcFlightProducer(
                schema,
                1024,
                2048,
                Duration.ofMillis(200),
                Clock.systemUTC(),
                3,
                1000,
                java.util.List.of(),
                java.util.List.of(),
                5_000_000,
                20_000_000,
                allocator,
                Location.forGrpcInsecure(HOST, flightPort),
                USER,
                PASSWORD,
                CATALOG,
                SCHEMA,
                Map.of("path", path),
                Duration.ofSeconds(30)
        )) {
            sender.addRow(new JavaRow(new Object[]{1}));
            sender.addRow(new JavaRow(new Object[]{2}));
            sender.addRow(new JavaRow(new Object[]{3}));
        }

        var query = "SELECT count(*) as cnt FROM read_parquet('%s/%s/*.parquet')".formatted(warehouse, path);
        FlightInfo flightInfo = client.execute(query);

        try (FlightStream stream = client.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
            while (stream.next()) {
                var root = stream.getRoot();
                var countVector = (org.apache.arrow.vector.BigIntVector) root.getVector("cnt");
                assertEquals(3L, countVector.get(0));
            }
        }
    }
}
