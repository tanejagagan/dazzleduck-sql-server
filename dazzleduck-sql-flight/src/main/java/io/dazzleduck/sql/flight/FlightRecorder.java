package io.dazzleduck.sql.flight;

import java.util.concurrent.Callable;

public interface FlightRecorder {

    void recordStatementCancel();

    void recordPreparedStatementCancel();

    <T> T recordGetFlightInfo(Callable<T> work) throws Exception;

    <T> T recordGetFlightInfoPrepared(Callable<T> work) throws Exception;

    void recordGetStreamStatement(Runnable r);

    void recordGetStreamPreparedStatement(Runnable r);

    void recordBulkIngest(Runnable r);

    void recordGetFlightInfoStatement();



    void incrementRunningPrepared();
    void decrementRunningPrepared();



    void recordGetFlightInfoPreparedStatement();

    void recordGetStreamPreparedStatement(long networkSize);



    // Add other methods which needs to be recorded
}

