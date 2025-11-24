package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.FlightRecorder;

public class NOOPFlightRecorder implements FlightRecorder {
    @Override
    public void recordGetFlightInfoStatement() {

    }

    @Override
    public void recordGetFlightInfoPreparedStatement() {

    }

    @Override
    public void recordGetStreamPreparedStatement(long networkSize) {

    }
}
