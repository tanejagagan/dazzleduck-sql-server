package io.dazzleduck.sql.commons.authorization;

public class UnauthorizedException extends Exception {
    public UnauthorizedException(String msg) {
        super(msg);
    }
}
