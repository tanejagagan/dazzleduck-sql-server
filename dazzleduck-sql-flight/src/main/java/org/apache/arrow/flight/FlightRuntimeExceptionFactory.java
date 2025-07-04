package org.apache.arrow.flight;

public class FlightRuntimeExceptionFactory {
    public static FlightRuntimeException of(CallStatus status) {
        return new FlightRuntimeException(status);
    }
}
