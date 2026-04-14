package io.dazzleduck.sql.common;

public class ParameterValidationException extends Exception {

    public ParameterValidationException(String message) {
        super(message);
    }

    public ParameterValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
