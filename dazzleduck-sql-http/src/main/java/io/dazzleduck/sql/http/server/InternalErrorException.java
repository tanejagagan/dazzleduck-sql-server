package io.dazzleduck.sql.http.server;

public class InternalErrorException extends HttpException {

    public InternalErrorException(int errorCode, String msg) {
        super(errorCode, msg);
    }
}
