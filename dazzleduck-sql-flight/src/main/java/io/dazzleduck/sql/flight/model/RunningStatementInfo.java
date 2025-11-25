package io.dazzleduck.sql.flight.model;

public class RunningStatementInfo {
    public final String user;
    public final String statementId;
    public String query;
    public final long startTimeMs;
    public volatile String action = "RUNNING";
    public volatile Long endTimeMs = null;
    public RunningStatementInfo(String user, String statementId, String query) {
        this.user = user;
        this.statementId = statementId;
        this.query = query;
        this.startTimeMs = System.currentTimeMillis();
    }

    public long durationMs() {
        long end = (endTimeMs != null) ? endTimeMs : System.currentTimeMillis();
        return end - startTimeMs;
    }
}
