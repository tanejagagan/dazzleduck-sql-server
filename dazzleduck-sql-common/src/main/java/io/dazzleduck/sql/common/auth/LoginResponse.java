package io.dazzleduck.sql.common.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class LoginResponse {
    public static final String BEARER_TOKEN_TYPE = "Bearer";

    @JsonProperty("accessToken")
    private final String accessToken;
    @JsonProperty("username")
    private final String username;
    @JsonProperty("tokenType")
    private final String tokenType;

    @JsonCreator
    public LoginResponse(
            @JsonProperty("accessToken") String accessToken,
            @JsonProperty("username") String username,
            @JsonProperty("tokenType") String tokenType) {
        this.accessToken = accessToken;
        this.username = username;
        this.tokenType = tokenType != null ? tokenType : BEARER_TOKEN_TYPE;
    }

    public LoginResponse(String accessToken, String username) {
        this(accessToken, username, BEARER_TOKEN_TYPE);
    }

    public String accessToken() {
        return accessToken;
    }

    public String username() {
        return username;
    }

    public String tokenType() {
        return tokenType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoginResponse that = (LoginResponse) o;
        return Objects.equals(accessToken, that.accessToken) &&
               Objects.equals(username, that.username) &&
               Objects.equals(tokenType, that.tokenType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessToken, username, tokenType);
    }

    @Override
    public String toString() {
        return "LoginResponse[accessToken=***, username=" + username + ", tokenType=" + tokenType + "]";
    }
}
