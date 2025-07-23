package io.dazzleduck.sql.flight.server;

public class NoRegisterExecutorException extends Exception {
    private final String cluster;

    public NoRegisterExecutorException(String cluster) {
        super("No Register Executor for cluster " + cluster);
        this.cluster = cluster;
    }

    public String clusterId() {
        return cluster;
    }
}
