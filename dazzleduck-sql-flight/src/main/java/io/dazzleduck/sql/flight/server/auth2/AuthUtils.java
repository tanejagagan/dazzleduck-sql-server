package io.dazzleduck.sql.flight.server.auth2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import io.dazzleduck.sql.common.auth.Validator;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AuthUtils {

    // JWT cache: username -> token
    private static final Map<String, String> jwtCache = new ConcurrentHashMap<>();
    private static final BasicCallHeaderAuthenticator.CredentialValidator NO_AUTH_CREDENTIAL_VALIDATOR =
            (username, password) -> {
                if (!password.isEmpty()) {
                    return () -> username;
                } else {
                    throw new RuntimeException("Authentication failure");
                }
            };

    private static String generateBasicAuthHeader(String username, String password) {
        byte[] up = Base64.getEncoder().encode((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        return Auth2Constants.BASIC_PREFIX +
                new String(up);
    }

    public static CallHeaderAuthenticator getAuthenticator(Config config) throws NoSuchAlgorithmException {
        BasicCallHeaderAuthenticator.CredentialValidator validator = createCredentialValidator(config);
        CallHeaderAuthenticator authenticator = new BasicCallHeaderAuthenticator(validator);
        var secretKey = Validator.generateRandoSecretKey();
        return new GeneratedJWTTokenAuthenticator(authenticator, secretKey, config);
    }

    public static CallHeaderAuthenticator getAuthenticator() throws NoSuchAlgorithmException {
        CallHeaderAuthenticator authenticator = new BasicCallHeaderAuthenticator(NO_AUTH_CREDENTIAL_VALIDATOR);
        var secretKey = Validator.generateRandoSecretKey();
        return new GeneratedJWTTokenAuthenticator(authenticator, secretKey);
    }

    public static FlightClientMiddleware.Factory createClientMiddlewareFactory(String username,
                                                                               String password,
                                                                               Map<String, String> headers) {
        return new FlightClientMiddleware.Factory() {
            @Override
            public FlightClientMiddleware onCallStarted(CallInfo info) {
                return new FlightClientMiddleware() {
                    private String bearer = jwtCache.get(username);

                    @Override
                    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
                        if (bearer != null) {
                            outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, "Bearer " + bearer);
                        } else {
                            outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER,
                                    generateBasicAuthHeader(username, password));
                        }
                        headers.forEach(outgoingHeaders::insert);
                    }

                    @Override
                    public void onHeadersReceived(CallHeaders incomingHeaders) {
                        String newToken = incomingHeaders.get(Auth2Constants.AUTHORIZATION_HEADER);
                        if (newToken != null && newToken.startsWith("Bearer ")) {
                            jwtCache.put(username, newToken.substring("Bearer ".length()));
                        }
                    }

                    @Override
                    public void onCallCompleted(CallStatus status) {
                    }
                };
            }
        };
    }

    public static BasicCallHeaderAuthenticator.CredentialValidator createCredentialValidator(Config config) {
        if (config.getBoolean("httpLogin")) {
            String loginUrl = config.getString("login.url");
            return createCredentialValidatorWithHttp(loginUrl);
        }

        List<? extends ConfigObject> users = config.getObjectList("users");
        Map<String, String> passwords = new HashMap<>();
        users.forEach(o -> {
            String name = o.toConfig().getString("username");
            String password = o.toConfig().getString("password");
            passwords.put(name, password);
        });

        return createCredentialValidatorWithFlight(passwords);
    }

    private static BasicCallHeaderAuthenticator.CredentialValidator createCredentialValidatorWithHttp(String loginUrl) {
        HttpClient client = HttpClient.newHttpClient();
        ObjectMapper objectMapper = new ObjectMapper(); // new Jackson ObjectMapper

        return (username, password) -> {
            try {
                Map<String, Object> bodyMap = Map.of(
                        "username", username,
                        "password", password,
                        "claims", Map.of()
                );

                String jsonBody = objectMapper.writeValueAsString(bodyMap);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(loginUrl))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                        .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    String jwt = response.body();
                    jwtCache.put(username, jwt);
                    return () -> username;
                } else {
                    throw new RuntimeException("Authentication failed via HTTP: " + response.statusCode());
                }
            } catch (Exception e) {
                throw new RuntimeException("HTTP login request failed", e);
            }
        };
    }

    private static BasicCallHeaderAuthenticator.CredentialValidator createCredentialValidatorWithFlight(Map<String, String> userPassword) {
        Map<String, byte[]> userHashMap = new HashMap<>();
        userPassword.forEach((u, p) -> userHashMap.put(u, Validator.hash(p)));

        return (username, password) -> {
            byte[] storedHash = userHashMap.get(username);
            if (storedHash != null && password != null && !password.isEmpty() && Validator.passwordMatch(storedHash, Validator.hash(password))) {
                return () -> username;
            } else {
                throw new RuntimeException("Authentication failure");
            }
        };
    }
}
