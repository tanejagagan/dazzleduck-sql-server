package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.authorization.AccessMode;
import io.dazzleduck.sql.common.authorization.SqlAuthorizer;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.MappedReader;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.flight.FlightStreamReader;
import io.dazzleduck.sql.flight.server.auth2.AdvanceJWTTokenAuthenticator;
import io.dazzleduck.sql.flight.server.auth2.AdvanceServerCallHeaderAuthMiddleware;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

public interface FlightTestUtils {

    String testSchema();
    String testCatalog();
    String USER = "admin";
    String PASSWORD = "password";
    default String testUser() {
        return USER;
    }
    default String testUserPassword() {
        return PASSWORD;
    }

    default ServerClient createRestrictedServerClient(Location serverLocation, SqlAuthorizer authorizer) throws IOException, NoSuchAlgorithmException {
        return createRestrictedServerClient(serverLocation, authorizer, AuthUtils.getTestAuthenticator(), Map.of());
    }
    default ServerClient createRestrictedServerClient(Location serverLocation, SqlAuthorizer authorizer, AdvanceJWTTokenAuthenticator testAuthenticator, Map<String, String> additionalHeader) throws IOException, NoSuchAlgorithmException {
        var warehousePath = Files.createTempDirectory("duckdb_warehouse_" + DuckDBFlightSqlProducerTest.class.getName()).toString();
        var clientAllocator = new RootAllocator();
        var serverAllocator = new RootAllocator();

        var restrictFlightServer = FlightServer.builder(
                        serverAllocator,
                        serverLocation,
                        new DuckDBFlightSqlProducer(serverLocation,
                                UUID.randomUUID().toString(),
                                "change me",
                                serverAllocator, warehousePath, AccessMode.RESTRICTED, authorizer))
                .middleware(AdvanceServerCallHeaderAuthMiddleware.KEY,
                        new AdvanceServerCallHeaderAuthMiddleware.Factory(testAuthenticator))
                .build()
                .start();

        var allHeader = new HashMap<String, String >();
        allHeader.putAll(Map.of(Headers.HEADER_DATABASE, testCatalog(),
                Headers.HEADER_SCHEMA, testSchema()));
        allHeader.putAll(additionalHeader);
        var restrictSqlClient = new FlightSqlClient(FlightClient.builder(clientAllocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(testUser(),
                        testUserPassword(),
                        allHeader))
                .build());
        return new ServerClient(restrictFlightServer, restrictSqlClient, clientAllocator, warehousePath);
    }

    static ServerClient setUpFlightServerAndClient(String[] confOverload,
                                                   String user,
                                                   String password,
                                                   Map<String, String> clientHeaders) throws Exception {
        var flightServer = io.dazzleduck.sql.flight.server.Main.createServer(confOverload);
        Thread severThread = new Thread(() -> {
            try {
                flightServer.start();
                System.out.println("Flight Server is up: Listening on URI: " + flightServer.getLocation().getUri());
                flightServer.awaitTermination();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        severThread.start();
        var clientAllocator = new RootAllocator();
        var sqlClient = new FlightSqlClient(FlightClient.builder(clientAllocator, flightServer.getLocation())
                .intercept(AuthUtils.createClientMiddlewareFactory(user, password, clientHeaders))
                .build());
        var commandLineConfig = ConfigUtils.loadCommandLineConfig(confOverload).config();
        return new ServerClient(flightServer, sqlClient, clientAllocator, ConfigUtils.getWarehousePath(commandLineConfig));
    }

    static void testQuery(String testQuery, FlightSqlClient sqlClient, BufferAllocator clientAllocator) throws Exception {
        testQuery(testQuery, testQuery, sqlClient, clientAllocator);
    }

    static void testQuery(String expectedQuery, String query, FlightSqlClient sqlClient, BufferAllocator clientAllocator) throws Exception {
        testStream(expectedQuery, () -> {
            final FlightInfo flightInfo = sqlClient.execute(query);
            return sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket());
        },  clientAllocator);
    }

    static void testStream(String expectedQuery, Supplier<FlightStream> streamSupplier, BufferAllocator clientAllocator) throws Exception {
        try (final FlightStream stream = streamSupplier.get()) {
            TestUtils.isEqual(expectedQuery, clientAllocator, FlightStreamReader.of(stream, clientAllocator));
        }
    }
}
