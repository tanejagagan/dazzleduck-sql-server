package io.dazzleduck.sql.common.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.auth.UnauthorizedException;

public class NOOPAuthorizer implements SqlAuthorizer {
    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query) throws UnauthorizedException {
        return query;
    }
}
