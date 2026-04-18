package io.dazzleduck.sql.examples;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;

/**
 * Generates minimal JWT tokens for integration tests against servers
 * configured with jwt_token.verify_signature=false.
 */
class TestTokens {

    private TestTokens() {}

    /**
     * Returns a minimal HS256 JWT signed with a throwaway key.
     * Works with any DazzleDuck server running verify_signature=false.
     */
    static String unsignedToken() {
        String header = base64url("{\"alg\":\"HS256\",\"typ\":\"JWT\"}");
        long exp = (System.currentTimeMillis() / 1000) + 3600;
        String payload = base64url("{\"sub\":\"test\",\"exp\":" + exp + "}");
        // Signature is ignored by the server — use a fixed dummy value
        String sig = "dummysignature";
        return header + "." + payload + "." + sig;
    }

    private static String base64url(String json) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }
}
