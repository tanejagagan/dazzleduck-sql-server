package io.dazzleduck.sql.flight.context;

import io.dazzleduck.sql.flight.server.auth2.AdvanceServerCallHeaderAuthMiddleware;
import io.dazzleduck.sql.flight.server.auth2.AuthResultWithClaims;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.Auth2Constants;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class SyntheticFlightContext implements FlightProducer.CallContext {
    private final CallHeaders callHeaders;
    private final String peerIdentity;

    private final Map<String, String> verifiedClaims;
    private final Map<FlightServerMiddleware.Key<?>, FlightServerMiddleware> middlewareMap;

    public SyntheticFlightContext(Map<String, List<String>> headers,
                                  Map<String, String> verifiedClaims) {
        this(headers, verifiedClaims, Map.of());
    }
    public SyntheticFlightContext(Map<String, List<String>> headers,
                                  Map<String, String> verifiedClaims,
                                  Map<String, List<String>> parameters) {
        this.callHeaders = new FlightCallHeaders();
        this.verifiedClaims = verifiedClaims;
        headers.forEach((k, vs) -> vs.forEach(v -> callHeaders.insert(k, v)));
        parameters.forEach((k, vs) -> vs.forEach(v -> callHeaders.insert(k, v)));
        verifiedClaims.forEach(callHeaders::insert);
        ServerHeaderMiddleware serverHeaderMiddleware = new ServerHeaderMiddleware.Factory()
                .onCallStarted(null, callHeaders, null);
        middlewareMap = Map.of(FlightConstants.HEADER_KEY, serverHeaderMiddleware);
        var authEncodedList =
                headers.get(Auth2Constants.AUTHORIZATION_HEADER);
        var _peerIdentity = "";
        if (authEncodedList != null && !authEncodedList.isEmpty()) {
            var authEncoded = authEncodedList.get(0)
                    .substring(Auth2Constants.BASIC_PREFIX.length() + 1);
            // The value has the format Base64(<username>:<password>)
            final String authDecoded =
                    new String(Base64.getDecoder().decode(authEncoded), StandardCharsets.UTF_8);
            final int colonPos = authDecoded.indexOf(':');
            if (colonPos != -1) {
                _peerIdentity = authDecoded.substring(0, colonPos);
            }
        }
        peerIdentity = _peerIdentity;
    }

    @Override
    public String peerIdentity() {
        return peerIdentity;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public <T extends FlightServerMiddleware> T getMiddleware(FlightServerMiddleware.Key<T> key) {
        //noinspection unchecked
        if (key.equals(AdvanceServerCallHeaderAuthMiddleware.KEY)) {
            return (T) new AdvanceServerCallHeaderAuthMiddleware(new AuthResultWithClaims(peerIdentity, "", this.verifiedClaims));
        } else {
            return (T) middlewareMap.get(key);
        }
    }

    @Override
    public Map<FlightServerMiddleware.Key<?>, FlightServerMiddleware> getMiddleware() {
        return Map.of();
    }
}