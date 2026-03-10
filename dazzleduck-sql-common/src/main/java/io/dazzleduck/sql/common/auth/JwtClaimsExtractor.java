package io.dazzleduck.sql.common.auth;

import io.dazzleduck.sql.common.Headers;
import io.jsonwebtoken.Claims;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JwtClaimsExtractor {

    /**
     * Extracts all relevant claims from a JWT payload into a flat map.
     * Includes configured claim headers, token_type, redirect_url (if present),
     * and the raw bearer token.
     */
    public static Map<String, String> extractClaims(Claims payload, List<String> claimHeader, String bearerToken) {
        var allClaimsFromJWT = new HashMap<String, String>();
        for (String key : claimHeader) {
            var claimFromJwt = payload.get(key, String.class);
            allClaimsFromJWT.put(key, claimFromJwt);
        }
        var tokenType = payload.get(Headers.HEADER_TOKEN_TYPE, String.class);
        allClaimsFromJWT.put(Headers.HEADER_TOKEN_TYPE, tokenType != null ? tokenType : Headers.HEADER_TOKEN_INLINE);
        var redirectUrl = payload.get(Headers.HEADER_REDIRECT_URL, String.class);
        if (redirectUrl != null) {
            allClaimsFromJWT.put(Headers.HEADER_REDIRECT_URL, redirectUrl);
        }
        allClaimsFromJWT.put(Headers.HEADER_BEARER_TOKEN, bearerToken);
        return allClaimsFromJWT;
    }
}
