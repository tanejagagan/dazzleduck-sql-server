package io.dazzleduck.sql.http.server;

import java.util.Map;

public record LoginObject(String username, String password, Map<String, Object> claims) {

    public LoginObject {
        if (claims == null) {
            claims = Map.of();
        }
    }

    public LoginObject(String username, String password) {
        this(username, password, Map.of());
    }
}
