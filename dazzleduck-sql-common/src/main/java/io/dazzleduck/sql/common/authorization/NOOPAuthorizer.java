package io.dazzleduck.sql.common.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.auth.UnauthorizedException;

import java.util.Map;

public class NOOPAuthorizer implements SqlAuthorizer {
    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query,
                              Map<String, String> verifiedClaims) throws UnauthorizedException {
        return query;
    }
}
