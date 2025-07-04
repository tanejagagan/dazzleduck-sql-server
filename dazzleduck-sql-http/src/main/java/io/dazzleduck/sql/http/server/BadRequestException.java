package io.dazzleduck.sql.http.server;



public class BadRequestException extends HttpException {

    public BadRequestException(int errorCode, String msg) {
        super(errorCode, msg);
    }
}


