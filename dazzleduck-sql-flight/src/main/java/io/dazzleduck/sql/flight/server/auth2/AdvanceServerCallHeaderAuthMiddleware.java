package io.dazzleduck.sql.flight.server.auth2;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.ServerCallHeaderAuthMiddleware;

public class AdvanceServerCallHeaderAuthMiddleware extends ServerCallHeaderAuthMiddleware {


    public static FlightServerMiddleware.Key<AdvanceServerCallHeaderAuthMiddleware> KEY =
            FlightServerMiddleware.Key.of(Auth2Constants.AUTHORIZATION_HEADER);
    public static class Factory
            implements FlightServerMiddleware.Factory<AdvanceServerCallHeaderAuthMiddleware> {
        private final AdvanceJWTTokenAuthenticator authHandler;

        /**
         * Construct a factory with the given auth handler.
         *
         * @param authHandler The auth handler what will be used for authenticating requests.
         */
        public Factory(AdvanceJWTTokenAuthenticator authHandler) {
            this.authHandler = authHandler;
        }

        @Override
        public AdvanceServerCallHeaderAuthMiddleware onCallStarted(
                CallInfo callInfo, CallHeaders incomingHeaders, RequestContext context) {
            final AuthResultWithClaims result = authHandler.authenticate(incomingHeaders);
            context.put(Auth2Constants.PEER_IDENTITY_KEY, result.getPeerIdentity());
            return new AdvanceServerCallHeaderAuthMiddleware(result);
        }
    }

    private final AuthResultWithClaims authResultWithClaims;

    public AdvanceServerCallHeaderAuthMiddleware(AuthResultWithClaims authResultWithClaims) {
        super(authResultWithClaims);
        this.authResultWithClaims = authResultWithClaims;
    }
    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
        authResultWithClaims.appendToOutgoingHeaders(outgoingHeaders);
    }

    public AuthResultWithClaims getAuthResultWithClaims() {
        return authResultWithClaims;
    }
}
