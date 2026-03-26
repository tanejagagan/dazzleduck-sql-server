package io.dazzleduck.sql.flight.context;

import io.dazzleduck.sql.commons.authorization.SubjectAndVerifiedClaims;
import io.dazzleduck.sql.flight.server.auth2.AdvanceServerCallHeaderAuthMiddleware;
import io.dazzleduck.sql.flight.server.auth2.AuthResultWithClaims;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.Auth2Constants;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.dazzleduck.sql.common.auth.JwtClaimsExtractor.parseJwtClaims;

public class SyntheticFlightContext implements FlightProducer.CallContext {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final CallHeaders callHeaders;
    private final String peerIdentity;
    private final Map<FlightServerMiddleware.Key<?>, FlightServerMiddleware> middlewareMap;

    public SyntheticFlightContext(Map<String, List<String>> headers) {
        this(headers, null);
    }

    public SyntheticFlightContext(Map<String, List<String>> headers,
                                  @Nullable SubjectAndVerifiedClaims subjectAndVerifiedClaims) {
        this(headers, subjectAndVerifiedClaims, Map.of());
    }

    public SyntheticFlightContext(Map<String, List<String>> headers,
                                  @Nullable SubjectAndVerifiedClaims subjectAndVerifiedClaims,
                                  Map<String, List<String>> queryParameters) {
        this.callHeaders = new FlightCallHeaders();
        headers.forEach((k, vs) -> vs.forEach(v -> callHeaders.insert(k, v)));
        queryParameters.forEach((k, vs) -> vs.forEach(v -> callHeaders.insert(k, v)));

        if (subjectAndVerifiedClaims != null) {
            this.peerIdentity = subjectAndVerifiedClaims.subject();
        } else {
            this.peerIdentity = extractPeerIdentityFromAuth(headers);
        }

        AuthResultWithClaims authResultWithClaims = subjectAndVerifiedClaims != null
                ? new AuthResultWithClaims(peerIdentity, "", subjectAndVerifiedClaims.verifiedClaims())
                : new AuthResultWithClaims(peerIdentity, null, Map.of());

        ServerHeaderMiddleware serverHeaderMiddleware = new ServerHeaderMiddleware.Factory()
                .onCallStarted(null, callHeaders, null);

        Map<FlightServerMiddleware.Key<?>, FlightServerMiddleware> mutableMap = new HashMap<>();
        mutableMap.put(FlightConstants.HEADER_KEY, serverHeaderMiddleware);
        mutableMap.put(AdvanceServerCallHeaderAuthMiddleware.KEY,
                new AdvanceServerCallHeaderAuthMiddleware(authResultWithClaims));
        this.middlewareMap = Map.copyOf(mutableMap);
    }

    public static String extractPeerIdentityFromAuth(Map<String, List<String>> headers) {
        List<String> authHeaders = headers.getOrDefault(Auth2Constants.AUTHORIZATION_HEADER,
                headers.get(Auth2Constants.AUTHORIZATION_HEADER.toLowerCase()));

        if (authHeaders == null || authHeaders.isEmpty()) {
            return null;
        }

        String authHeader = authHeaders.get(0);

        if (authHeader.startsWith(Auth2Constants.BEARER_PREFIX)) {
            // JWT format: header.payload.signature (each part Base64URL-encoded)
            // Extract subject from payload without verifying signature (already verified by middleware)
            String bearerToken = authHeader.substring(Auth2Constants.BEARER_PREFIX.length()).trim();
            var payload = parseJwtClaims(bearerToken);
            try {
                return payload.getSubject();
            } catch (Exception e) {
                return null;
            }
        } else {
            // Basic auth: base64(username:password)
            String encodedCredentials = authHeader.substring(Auth2Constants.BASIC_PREFIX.length()).trim();
            String decodedCredentials = new String(
                    Base64.getDecoder().decode(encodedCredentials),
                    StandardCharsets.UTF_8
            );
            int colonIndex = decodedCredentials.indexOf(':');
            if (colonIndex == -1) {
                return null;
            }
            return decodedCredentials.substring(0, colonIndex);
        }
    }

    public CallHeaders getCallHeaders() {
        return callHeaders;
    }

    @Override
    public String peerIdentity() {
        return peerIdentity;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends FlightServerMiddleware> T getMiddleware(FlightServerMiddleware.Key<T> key) {
        return (T) middlewareMap.get(key);
    }

    @Override
    public Map<FlightServerMiddleware.Key<?>, FlightServerMiddleware> getMiddleware() {
        return middlewareMap;
    }
}