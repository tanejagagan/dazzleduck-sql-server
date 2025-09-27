package io.dazzleduck.sql.flight.server.auth2;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import java.util.Map;

public class AuthResultWithClaims implements CallHeaderAuthenticator.AuthResult {
    private final String subject;
    private final String bearerToken;
    private final Map<String, String> jwtVerifiedClaims;

    public AuthResultWithClaims(String subject, String bearerToken, Map<String, String> jwtVerifiedClaims) {
        this.subject = subject;
        this.bearerToken = bearerToken;
        this.jwtVerifiedClaims = jwtVerifiedClaims;
    }

    @Override
    public String getPeerIdentity() {
        return subject;
    }

    @Override
    public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
        outgoingHeaders.insert(
                Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + bearerToken);
    }


    public Map<String, String> verifiedClaims() {
        return jwtVerifiedClaims;
    }
}
