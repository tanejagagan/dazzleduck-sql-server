package io.dazzleduck.sql.flight;

public interface FlightRecorder {

    void recordGetFlightInfoStatement();

    void recordGetFlightInfoPreparedStatement();

    void recordGetStreamPreparedStatement(long networkSize);

    // Add other methods which needs to be recorded
}


