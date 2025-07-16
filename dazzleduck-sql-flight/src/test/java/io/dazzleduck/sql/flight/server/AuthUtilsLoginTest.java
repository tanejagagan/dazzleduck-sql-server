package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.common.authorization.AccessMode;
import io.dazzleduck.sql.common.authorization.NOOPAuthorizer;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.dazzleduck.sql.http.server.Main;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AuthUtilsLoginTest {
    private static final String USER = "admin";
    private static final String PASSWORD = "password";
    protected static final String LOCALHOST = "localhost";
    private static final BufferAllocator clientAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final BufferAllocator serverAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final int HTTP_PORT = 8080;
    protected static FlightServer flightServer;
    protected static FlightSqlClient sqlClient;
    protected static FlightSqlClient client;
    private static RootAllocator allocator;
    private static String warehousePath;

    @BeforeAll
    public static void setup() throws Exception {
        warehousePath = Files.createTempDirectory("duckdb_warehouse_" + DuckDBFlightSqlProducerTest.class.getName()).toString();
        Main.main(new String[]{
                "--conf", "port=" + HTTP_PORT,
                "--conf", "warehousePath=" + warehousePath
        });

       setUpClientServer();
    }

    private static void setUpClientServer() throws Exception {
        final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 55556);
        flightServer = FlightServer.builder(
                        serverAllocator,
                        serverLocation,
                        new DuckDBFlightSqlProducer(serverLocation,
                                UUID.randomUUID().toString(),
                                "change me",
                                serverAllocator, warehousePath, AccessMode.COMPLETE,
                                new NOOPAuthorizer()))
                .headerAuthenticator(AuthUtils.getAuthenticator())
                .build()
                .start();
        sqlClient = new FlightSqlClient(FlightClient.builder(clientAllocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER,
                        PASSWORD,
                        Map.of()))
                .build());
    }

    @Test
    public void testLogin() throws Exception {
        var stream = sqlClient.getCatalogs();
        assertNotNull(stream);

        // Access the middleware field from the client using reflection
        Field middlewareField = sqlClient.getClass().getDeclaredField("client");
        middlewareField.setAccessible(true);
        Object flightClient = middlewareField.get(sqlClient);

        Field middlewareListField = flightClient.getClass().getDeclaredField("middleware");
        middlewareListField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<?> middlewares = (List<?>) middlewareListField.get(flightClient);

        // Assuming the first middleware is yours
        Object middleware = middlewares.get(0);
        Field bearerField = middleware.getClass().getDeclaredField("bearer");
        bearerField.setAccessible(true);
        String jwt = (String) bearerField.get(middleware);

        assertNotNull(jwt);
        assert jwt.startsWith("Bearer ");

        String[] parts = jwt.replace("Bearer ", "").split("\\.");
        assertEquals(3, parts.length, "JWT should have 3 parts");

        String payload = new String(Base64.getUrlDecoder().decode(parts[1]));
        System.out.println("Decoded Payload: " + payload);
    }


    @AfterAll
    public static void cleanup() throws Exception {
        if (client != null) client.close();
        if (allocator != null) allocator.close();
    }
}
