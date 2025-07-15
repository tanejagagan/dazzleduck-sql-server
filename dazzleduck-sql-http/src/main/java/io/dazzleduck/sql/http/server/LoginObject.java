package io.dazzleduck.sql.http.server;

import java.util.Map;

public record LoginObject(String username, String password, Map<String, Object> claims) {}
