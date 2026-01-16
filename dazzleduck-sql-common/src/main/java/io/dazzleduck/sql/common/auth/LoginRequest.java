package io.dazzleduck.sql.common.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class LoginRequest {
    @JsonProperty("username")
    private final String username;
    @JsonProperty("password")
    private final String password;
    @JsonProperty("claims")
    private final Map<String, String> claims;

    @JsonCreator
    public LoginRequest(
            @JsonProperty("username") String username,
            @JsonProperty("password") String password,
            @JsonProperty("claims") Map<String, String> claims) {
        this.username = username;
        this.password = password;
        this.claims = claims != null ? claims : new HashMap<>();
    }

    public LoginRequest(String username, String password) {
        this(username, password, new HashMap<>());
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    public Map<String, String> claims() {
        return claims;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoginRequest that = (LoginRequest) o;
        return Objects.equals(username, that.username) &&
               Objects.equals(password, that.password) &&
               Objects.equals(claims, that.claims);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, password, claims);
    }

    @Override
    public String toString() {
        return "LoginRequest[username=" + username + ", password=***, claims=" + claims + "]";
    }
}
