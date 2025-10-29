package io.dazzleduck.sql.login;

import java.util.HashMap;
import java.util.Map;

public record LoginObject(String username, String password, Map<String, String> claims) {
    public LoginObject(String username, String password) {
        this(username, password, new HashMap<>());
    }
}
