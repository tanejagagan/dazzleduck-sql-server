package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.FlightRecorder;

import java.util.concurrent.Callable;

public class NOOPFlightRecorder implements FlightRecorder {
    @Override
    public void recordStatementCancel() {

    }

    @Override
    public void recordPreparedStatementCancel() {

    }

    @Override
    public <T> T recordGetFlightInfo(Callable<T> work) throws Exception {
        return null;
    }

    @Override
    public <T> T recordGetFlightInfoPrepared(Callable<T> work) throws Exception {
        return null;
    }

    @Override
    public void recordGetStreamStatement(Runnable r) {

    }

    @Override
    public void recordGetStreamPreparedStatement(Runnable r) {

    }

    @Override
    public void recordBulkIngest(Runnable r) {

    }

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