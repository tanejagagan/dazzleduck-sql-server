package io.dazzleduck.sql.flight.server.auth2;

import com.google.common.base.Strings;
import com.typesafe.config.Config;
import io.grpc.Metadata;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeExceptionFactory;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.grpc.MetadataAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class AdvanceJWTTokenAuthenticator implements CallHeaderAuthenticator {

    private static final Logger logger = LoggerFactory.getLogger(AdvanceBasicCallHeaderAuthenticator.class);
    private final SecretKey key;
    private final JwtParser jwtParser;
    private final Duration timeMinutes;
    private final CallHeaderAuthenticator initialAuthenticator;
    private final List<String> claimHeader;
    private final boolean generateToken;


    public AdvanceJWTTokenAuthenticator(CallHeaderAuthenticator initialAuthenticator, SecretKey key) {
        this.key = key;
        this.jwtParser = Jwts.parser()     // (1)
                .verifyWith(key) //     or a constant key used to verify all signed JWTs
                .build();
        this.timeMinutes = Duration.ofMinutes(60);
        this.generateToken = true;
        this.claimHeader = List.of();
        this.initialAuthenticator = initialAuthenticator;
    }

    public AdvanceJWTTokenAuthenticator(CallHeaderAuthenticator initialAuthenticator, SecretKey key, Config config) {
        this.key = key;
        this.jwtParser = Jwts.parser()     // (1)
                .verifyWith(key)//     or a constant key used to verify all signed JWTs
                .build();
        this.timeMinutes = config.getDuration("jwt.token.expiration");
        this.initialAuthenticator = initialAuthenticator;
        this.claimHeader = config.getStringList("jwt.token.claims.headers");
        this.generateToken = config.getBoolean("jwt.token.generation.enabled");
    }


    @Override
    public AuthResult authenticate(CallHeaders incomingHeaders) {
        // Check if headers contain a bearer token and if so, validate the token.
        final String bearerToken =
                AuthUtilities.getValueFromAuthHeader(incomingHeaders, Auth2Constants.BEARER_PREFIX);
        if (bearerToken != null) {
            return validateBearer(bearerToken, incomingHeaders);
        }

        // Delegate to the basic auth handler to do the validation.
        final CallHeaderAuthenticator.AuthResult result =
                initialAuthenticator.authenticate(incomingHeaders);
        return getAuthResultWithBearerToken(result, incomingHeaders);
    }


    protected AuthResult getAuthResultWithBearerToken(AuthResult authResult, CallHeaders incomingHeaders) {

        // We generate a dummy header and call appendToOutgoingHeaders with it.
        // We then inspect the dummy header and parse the bearer token if present in the header
        // and generate a new bearer token if a bearer token is not present in the header.
        final CallHeaders dummyHeaders = new MetadataAdapter(new Metadata());
        authResult.appendToOutgoingHeaders(dummyHeaders);
        String bearerToken = AuthUtilities.getValueFromAuthHeader(dummyHeaders, Auth2Constants.BEARER_PREFIX);

        if (!Strings.isNullOrEmpty(bearerToken)) {
            Token.token = bearerToken;
            return authResult; // Already has JWT from AuthUtils
        } else if (!generateToken) {
            logger.error("Authentication failed delegate token is missing");
            throw CallStatus.INTERNAL.toRuntimeException();
        }

        // Else, generate JWT manually (for Flight-based login)
        Calendar expiration = Calendar.getInstance();
        expiration.add(Calendar.MINUTE, (int) timeMinutes.toMinutes());

        var builder = Jwts.builder()
                .subject(authResult.getPeerIdentity())
                .expiration(expiration.getTime())
                .signWith(key);
        for (String key : claimHeader) {
            builder.claim(key, incomingHeaders.get(key));
        }
        String jwt = builder
                .compact();
        Token.token = jwt;

        return new AuthResult() {
            @Override
            public String getPeerIdentity() {
                return authResult.getPeerIdentity();
            }

            @Override
            public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
                authResult.appendToOutgoingHeaders(outgoingHeaders);
                outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + jwt);
            }
        };
    }


    protected AuthResult validateBearer(String bearerToken, CallHeaders incomingHeader) {
        try {
            var jwt = jwtParser.parseSignedClaims(bearerToken);
            var payload = jwt.getPayload();
            var subject = payload.getSubject();
            var expiration = payload.getExpiration();
            if (expiration.before(new Date())) {
                throw FlightRuntimeExceptionFactory.of(new CallStatus(CallStatus.UNAUTHENTICATED.code(), null, "Expired", null));
            }
            for (String key : claimHeader) {
                var claimsFromJwt = payload.get(key, String.class);
                var incomingHeaderValue = incomingHeader.get(key);
                if (!claimsFromJwt.equals(incomingHeaderValue)) {
                    throw FlightRuntimeExceptionFactory.of(new CallStatus(CallStatus.UNAUTHENTICATED.code(), null, "jwt and headers do not match", null));
                }
            }
            return new AuthResult() {
                @Override
                public String getPeerIdentity() {
                    return subject;
                }

                @Override
                public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
                    outgoingHeaders.insert(
                            Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + bearerToken);
                }
            };
        } catch (Exception e) {
            throw CallStatus.UNAUTHENTICATED.toRuntimeException();
        }
    }
}
