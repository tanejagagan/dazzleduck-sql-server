package io.dazzleduck.sql.flight.model;

public record FlightMetricsSnapshot(

        // ---- Application / Global ----
        long startTimeMs,

        // ---- Live gauges ----
        int runningStatements,
        int runningPrepared,
        int runningBulkIngest,

        // ---- Counters ----
        double completedStatements,
        double completedPrepared,
        double completedBulkIngest,

        double cancelledStatements,
        double cancelledPrepared

) {}
