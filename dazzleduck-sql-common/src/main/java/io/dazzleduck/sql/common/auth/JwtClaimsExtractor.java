package io.dazzleduck.sql.common.auth;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import io.dazzleduck.sql.common.Headers;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JwtClaimsExtractor {
    public static Claims parseJwtClaims(String token, JwtParser jwtParser, Boolean verifySignature) {
        try {
            if (verifySignature) {
                return jwtParser.parseSignedClaims(token).getPayload();
            } else {
                var unsecuredJwt = toUnsecuredJwt(token);
                return jwtParser.parseUnsecuredClaims(unsecuredJwt).getPayload();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JWT claims", e);
        }
    }

    public static Claims parseJwtClaims(String token) {
        try {
            JWTClaimsSet claimsSet = JWTParser.parse(token).getJWTClaimsSet();
            return Jwts.claims().add(claimsSet.getClaims()).build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JWT claims", e);
        }
    }

    public static Claims parseJwtClaims(String token, SecretKey secretKey) {
        try {
            var jwtParser = Jwts.parser()
                    .verifyWith(secretKey)
                    .build();
            return jwtParser.parseSignedClaims(token).getPayload();
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JWT claims", e);
        }
    }

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

    public static String toUnsecuredJwt(String token) {
        int firstDot = token.indexOf('.');
        int secondDot = token.indexOf('.', firstDot + 1);
        if (firstDot < 0 || secondDot < 0) {
            throw new IllegalArgumentException("Invalid JWT");
        }
        String payload = token.substring(firstDot + 1, secondDot);
        String unsecuredHeader = Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString("{\"alg\":\"none\"}".getBytes(StandardCharsets.UTF_8));
        return unsecuredHeader + "." + payload + ".";
    }
}
