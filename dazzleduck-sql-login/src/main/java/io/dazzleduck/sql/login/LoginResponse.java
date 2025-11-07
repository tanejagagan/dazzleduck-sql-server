package io.dazzleduck.sql.login;

public record LoginResponse(String accessToken, String username, String tokenType) {
    public static final String BEARER_TOKEN_TYPE = "Bearer";

    public LoginResponse(String accessToken, String username) {
        this(accessToken, username, BEARER_TOKEN_TYPE);
    }
}
