package io.dazzleduck.sql.login;

import java.util.HashMap;
import java.util.Map;

public record LoginRequest(String username, String password, Map<String, String> claims) {
    public LoginRequest(String username, String password) {
        this(username, password, new HashMap<>());
    }
}
