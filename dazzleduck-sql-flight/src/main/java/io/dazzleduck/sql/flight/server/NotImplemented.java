package io.dazzleduck.sql.flight.server;

public class NotImplemented extends Exception {
    public NotImplemented(String function) {
        super("Functionality Not Implemented: " + function);
    }
}
