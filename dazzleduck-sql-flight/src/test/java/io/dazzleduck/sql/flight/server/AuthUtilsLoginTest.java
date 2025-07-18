package io.dazzleduck.sql.flight.server;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.authorization.AccessMode;
import io.dazzleduck.sql.common.authorization.NOOPAuthorizer;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.dazzleduck.sql.http.server.Main;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class AuthUtilsLoginTest {
    private static final String USER = "admin";
    private static final String PASSWORD = "admin"; // for httpLogin
    private static final String LOCALHOST = "localhost";
    private static final BufferAllocator clientAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final BufferAllocator serverAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final int HTTP_PORT = 8080;
    private static FlightServer flightServer;
    private static FlightSqlClient sqlClient;
    private static RootAllocator allocator;
    private static String warehousePath;

    @BeforeAll
    public static void setup() throws Exception {
        warehousePath = Files.createTempDirectory("duckdb_warehouse_" + AuthUtilsLoginTest.class.getSimpleName()).toString();
        // Start the HTTP login service
        Main.main(new String[]{
                "--conf", "port=" + HTTP_PORT,
                "--conf", "warehousePath=" + warehousePath
        });

        setUpFlightServerAndClient();
    }

    private static void setUpFlightServerAndClient() throws Exception {
        final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 55556);

        flightServer = FlightServer.builder(
                        serverAllocator,
                        serverLocation,
                        new DuckDBFlightSqlProducer(serverLocation,
                                UUID.randomUUID().toString(),
                                "change me",
                                serverAllocator,
                                warehousePath,
                                AccessMode.COMPLETE,
                                new NOOPAuthorizer()))
                .headerAuthenticator(AuthUtils.getAuthenticator()) // Uses httpLogin or fallback
                .build()
                .start();

        sqlClient = new FlightSqlClient(FlightClient.builder(clientAllocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER, PASSWORD, Map.of()))
                .build());
    }

    @Test
    public void testLoginWithHttp() throws Exception {
        Config config = ConfigFactory.load().getConfig("dazzleduck-flight-server");

        if (config.getBoolean("httpLogin")) {
            var validator = AuthUtils.createCredentialValidator(config);
            var result = validator.validate(USER, PASSWORD);

            assertNotNull(result);
            assertEquals(USER, result.getPeerIdentity());

            CallHeaders headers = new FlightCallHeaders();
            result.appendToOutgoingHeaders(headers);

            String authHeader = headers.get(Auth2Constants.AUTHORIZATION_HEADER);
            assertNotNull(authHeader);
            assertTrue(authHeader.startsWith("Bearer "), "Authorization header must start with Bearer");

            String jwt = authHeader.substring("Bearer ".length());
            String[] parts = jwt.split("\\.");
            assertEquals(3, parts.length, "JWT should have 3 parts (header, payload, signature)");

            String payload = new String(Base64.getUrlDecoder().decode(parts[1]));
            System.out.println("JWT Payload: " + payload);
        }
    }

    @Test
    public void testLoginWithFlight() throws Exception {
        Config config = ConfigFactory.load().getConfig("dazzleduck-flight-server");

        if (!config.getBoolean("httpLogin")) {
            // Create Basic Auth Header
            String credentials = USER + ":" + PASSWORD;
            String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
            String basicAuthHeader = "Basic " + encoded;

            // Insert into CallHeaders
            CallHeaders headers = new FlightCallHeaders();
            headers.insert(Auth2Constants.AUTHORIZATION_HEADER, basicAuthHeader);

            // Authenticate
            CallHeaderAuthenticator authenticator = AuthUtils.getAuthenticator(config);
            CallHeaderAuthenticator.AuthResult authResult = authenticator.authenticate(headers);

            assertNotNull(authResult);
            assertEquals(USER, authResult.getPeerIdentity());

            // Extract token from AuthResult
            CallHeaders outHeaders = new FlightCallHeaders();
            authResult.appendToOutgoingHeaders(outHeaders);
            String authHeader = outHeaders.get(Auth2Constants.AUTHORIZATION_HEADER);

            assertNotNull(authHeader);
            assertTrue(authHeader.startsWith("Bearer "), "Expected Bearer token in header");

            String jwt = authHeader.substring("Bearer ".length());
            String[] parts = jwt.split("\\.");
            assertEquals(3, parts.length, "JWT should have 3 parts (header, payload, signature)");

            String payloadJson = new String(Base64.getUrlDecoder().decode(parts[1]));
            System.out.println("Generated JWT Payload (Flight): " + payloadJson);
        } else {
            System.out.println("Skipping testLoginWithFlight: httpLogin is enabled.");
        }
    }

    @AfterAll
    public static void cleanup() throws Exception {
        if (sqlClient != null) sqlClient.close();
        if (flightServer != null) flightServer.close();
        if (clientAllocator != null) clientAllocator.close();
        if (serverAllocator != null) serverAllocator.close();
    }
}
