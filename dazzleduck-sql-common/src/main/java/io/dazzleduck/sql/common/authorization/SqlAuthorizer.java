package io.dazzleduck.sql.common.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.UnauthorizedException;

public interface SqlAuthorizer {
    JsonNode authorize(String user, String database, String schema, JsonNode query) throws UnauthorizedException;
}
