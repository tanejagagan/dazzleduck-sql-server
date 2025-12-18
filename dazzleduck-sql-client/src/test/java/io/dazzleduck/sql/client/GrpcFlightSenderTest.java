package io.dazzleduck.sql.client;


import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.types.JavaRow;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.FlightRecorder;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.optimizer.QueryOptimizer;
import io.dazzleduck.sql.flight.server.DuckDBFlightSqlProducer;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTaskFactoryProvider;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.*;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GrpcFlightSenderTest {

    private static final String HOST = "localhost";
    private static final int PORT = 55620;

    private static final String USER = "admin";
    private static final String PASSWORD = "password";
    private static final String CATALOG = "test_catalog";
    private static final String SCHEMA = "test_schema";

    private FlightServer server;
    private FlightSqlClient client;
    private RootAllocator allocator;
    private String warehouse;

    @BeforeAll
    void startServer() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        warehouse = Files.createTempDirectory("grpc-test").toString();

        ConnectionPool.executeBatch(new String[]{
                "INSTALL arrow FROM community",
                "LOAD arrow",
                "ATTACH ':memory:' AS test_catalog",
                "CREATE SCHEMA test_catalog.test_schema"
        });

        Location location = Location.forGrpcInsecure(HOST, PORT);
        FlightRecorder recorder = new MicroMeterFlightRecorder(new io.micrometer.core.instrument.simple.SimpleMeterRegistry(), UUID.randomUUID().toString()
        );
        DuckDBFlightSqlProducer producer =
                new DuckDBFlightSqlProducer(
                        location,
                        UUID.randomUUID().toString(),
                        "secret",
                        allocator,
                        warehouse,
                        AccessMode.COMPLETE,
                        DuckDBFlightSqlProducer.newTempDir(),
                        PostIngestionTaskFactoryProvider.NO_OP.getPostIngestionTaskFactory(),
                        java.util.concurrent.Executors.newSingleThreadScheduledExecutor(),
                        Duration.ofMinutes(1),
                        Clock.systemUTC(),
                        recorder,
                        QueryOptimizer.NOOP_QUERY_OPTIMIZER
                );

        server = FlightServer.builder(allocator, location, producer)
                .headerAuthenticator(AuthUtils.getTestAuthenticator())
                .build()
                .start();

        client = new FlightSqlClient(FlightClient.builder(allocator, location)
                .intercept(AuthUtils.createClientMiddlewareFactory(
                                USER,
                                PASSWORD,
                                Map.of(
                                        Headers.HEADER_DATABASE, CATALOG,
                                        Headers.HEADER_SCHEMA, SCHEMA
                                )
                        ))
                        .build()
        );
    }

    @AfterAll
    void stopServer() throws Exception {
        client.close();
        server.close();
        allocator.close();
    }

    @Test
    void ingestAndVerifyValues() throws Exception {
        Schema schema = new Schema(List.of(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        String path = warehouse + "/names.parquet";

        try (GrpcFlightSender sender = new GrpcFlightSender(
                schema,
                1024,
                Duration.ofSeconds(5),
                Clock.systemUTC(),
                5_000_000,
                20_000_000,
                allocator,
                Location.forGrpcInsecure(HOST, PORT),
                USER,
                PASSWORD,
                CATALOG,
                SCHEMA,
                Map.of("path", path)
        )) {
            sender.addRow(new JavaRow(new Object[]{"Aman"}));
            sender.addRow(new JavaRow(new Object[]{"Yash"}));
            sender.addRow(new JavaRow(new Object[]{"Sid"}));
        }

        FlightInfo flightInfo = client.execute("SELECT name FROM '" + path + "' ORDER BY name");
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
}
