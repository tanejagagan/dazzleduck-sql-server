package io.dazzleduck.sql.http.server;

abstract public class HttpException extends RuntimeException {
    public int errorCode;
    public HttpException(int errorCode, String msg) {
        super(msg);
        this.errorCode = errorCode;
    }
}
