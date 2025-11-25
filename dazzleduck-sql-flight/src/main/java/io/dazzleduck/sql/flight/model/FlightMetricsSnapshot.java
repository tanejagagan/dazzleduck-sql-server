package io.dazzleduck.sql.flight.model;

public class FlightMetricsSnapshot {

    // ---- Application / Global ----
    public long startTimeMs;

    // ---- Live gauges ----
    public int runningStatements;
    public int runningPrepared;
    public int runningBulkIngest;

    // ---- Counters ----
    public double completedStatements;
    public double completedPrepared;
    public double completedBulkIngest;

    public double cancelledStatements;
    public double cancelledPrepared;
    public double dataInBytes;
    public double dataOutBytes;


    @Override
    public String toString() {
        return "FlightMetricsSnapshot{" +
                "startTimeMs=" + startTimeMs +
                ", runningStatements=" + runningStatements +
                ", runningPrepared=" + runningPrepared +
                ", runningBulkIngest=" + runningBulkIngest +
                ", completedStatements=" + completedStatements +
                ", completedPrepared=" + completedPrepared +
                ", completedBulkIngest=" + completedBulkIngest +
                ", cancelledStatements=" + cancelledStatements +
                ", cancelledPrepared=" + cancelledPrepared +
                '}';
    }
}
