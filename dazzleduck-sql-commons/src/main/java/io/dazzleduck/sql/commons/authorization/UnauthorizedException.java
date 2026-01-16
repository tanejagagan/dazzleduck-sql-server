package io.dazzleduck.sql.commons.authorization;

public class UnauthorizedException extends Exception {
    final String msg;
    public UnauthorizedException(String msg) {
        this.msg = msg;
    }
}
