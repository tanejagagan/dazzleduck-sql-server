package io.dazzleduck.sql.flight.server.auth2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.common.auth.LoginResponse;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpCredentialValidator implements AdvanceBasicCallHeaderAuthenticator.AdvanceCredentialValidator {


    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private final List<String> jwtClaims;
    private final ObjectMapper MAPPER = new ObjectMapper();
    private final String loginUrl;

    public HttpCredentialValidator(Config config) {
        this.jwtClaims = config.getStringList(ConfigConstants.JWT_TOKEN_CLAIMS_GENERATE_HEADERS_KEY);
        this.loginUrl = config.getString(ConfigConstants.LOGIN_URL_KEY);
    }

    @Override
    public CallHeaderAuthenticator.AuthResult validate(String username, String password, CallHeaders callHeaders) throws Exception {
        var claimMap = new HashMap<>();
        for (String claim : jwtClaims) {
            claimMap.put(claim, callHeaders.get(claim));
        }

        String requestBody = MAPPER.writeValueAsString(Map.of(
                "username", username,
                "password", password,
                "claims", claimMap
        ));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(loginUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to fetch token: " + response.body());
        }

        var result = MAPPER.readValue(response.body(), LoginResponse.class);

        return new CallHeaderAuthenticator.AuthResult() {
            @Override
            public String getPeerIdentity() {
                return username;
            }

            @Override
            public void appendToOutgoingHeaders(CallHeaders headers) {
                headers.insert(Auth2Constants.AUTHORIZATION_HEADER, result.tokenType() + " " + result.accessToken());
            }
        };
    }
}

