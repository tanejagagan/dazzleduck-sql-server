package io.dazzleduck.sql.otel.collector.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.auth.LoginResponse;
import io.dazzleduck.sql.commons.auth.Validator;
import io.grpc.*;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import io.dazzleduck.sql.common.SslUtils;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Calendar;
import java.util.Map;

public class JwtServerInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(JwtServerInterceptor.class);
    private static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String BASIC_PREFIX = "Basic ";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = SslUtils.trustAllHttpClient();

    private final SecretKey secretKey;
    private final JwtParser jwtParser;
    private final Map<String, byte[]> userHashMap;
    private final Duration jwtExpiration;
    private final String loginUrl;

    public JwtServerInterceptor(SecretKey secretKey, Map<String, byte[]> userHashMap,
                                Duration jwtExpiration, String loginUrl) {
        this.secretKey = secretKey;
        this.jwtParser = Jwts.parser()
                .verifyWith(secretKey)
                .build();
        this.userHashMap = userHashMap;
        this.jwtExpiration = jwtExpiration;
        this.loginUrl = loginUrl;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String authHeader = headers.get(AUTHORIZATION_KEY);
        if (authHeader == null) {
            call.close(Status.UNAUTHENTICATED.withDescription("Missing Authorization header"), new Metadata());
            return new ServerCall.Listener<>() {};
        }

        if (authHeader.startsWith(BASIC_PREFIX)) {
            return handleBasicAuth(authHeader.substring(BASIC_PREFIX.length()), call, headers, next);
        } else if (authHeader.startsWith(BEARER_PREFIX)) {
            return handleBearer(authHeader.substring(BEARER_PREFIX.length()), call, headers, next);
        } else {
            call.close(Status.UNAUTHENTICATED.withDescription("Unsupported Authorization scheme"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
    }

    private <ReqT, RespT> ServerCall.Listener<ReqT> handleBasicAuth(
            String encoded, ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        try {
            String decoded = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
            int colonPos = decoded.indexOf(':');
            if (colonPos == -1) {
                call.close(Status.UNAUTHENTICATED.withDescription("Invalid Basic auth format"), new Metadata());
                return new ServerCall.Listener<>() {};
            }
            String username = decoded.substring(0, colonPos);
            String password = decoded.substring(colonPos + 1);

            String token = loginUrl != null
                    ? delegateLogin(username, password)
                    : validateLocallyAndGenerateToken(username, password);

            String finalToken = token;
            ServerCall<ReqT, RespT> wrappedCall = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                @Override
                public void sendHeaders(Metadata responseHeaders) {
                    responseHeaders.put(AUTHORIZATION_KEY, finalToken);
                    super.sendHeaders(responseHeaders);
                }
            };
            return next.startCall(wrappedCall, headers);
        } catch (Exception e) {
            log.debug("Basic auth failed: {}", e.getMessage());
            call.close(Status.UNAUTHENTICATED.withDescription("Authentication failed"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
    }

    private String delegateLogin(String username, String password) throws Exception {
        String requestBody = MAPPER.writeValueAsString(Map.of(
                "username", username,
                "password", password
        ));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(loginUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
        HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("Login delegation failed: " + response.statusCode());
        }
        LoginResponse result = MAPPER.readValue(response.body(), LoginResponse.class);
        return result.tokenType() + " " + result.accessToken();
    }

    private String validateLocallyAndGenerateToken(String username, String password) {
        byte[] storedHash = userHashMap.get(username);
        if (storedHash == null || !Validator.passwordMatch(storedHash, Validator.hash(password))) {
            throw new RuntimeException("Invalid credentials");
        }
        return BEARER_PREFIX + generateToken(username);
    }

    private <ReqT, RespT> ServerCall.Listener<ReqT> handleBearer(
            String token, ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        try {
            jwtParser.parseSignedClaims(token);
            return next.startCall(call, headers);
        } catch (Exception e) {
            log.debug("JWT validation failed: {}", e.getMessage());
            call.close(Status.UNAUTHENTICATED.withDescription("Invalid or expired JWT token"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
    }

    private String generateToken(String subject) {
        Calendar expiration = Calendar.getInstance();
        expiration.add(Calendar.MINUTE, (int) jwtExpiration.toMinutes());
        return Jwts.builder()
                .subject(subject)
                .expiration(expiration.getTime())
                .signWith(secretKey)
                .compact();
    }
}
