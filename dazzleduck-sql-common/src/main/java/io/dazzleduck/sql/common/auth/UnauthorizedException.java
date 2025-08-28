package io.dazzleduck.sql.common.auth;

public class UnauthorizedException extends Exception {
    final String msg;
    public UnauthorizedException(String msg) {
        this.msg = msg;
    }
}
