package io.dazzleduck.sql.commons.ingestion;


import java.util.concurrent.Future;

public interface BulkIngestQueueInterface<T, R> extends AutoCloseable, IngestionStatsMBean {

    Future<R> add(Batch<T> batch);

    void write(WriteTask<T, R> writeTask);

    /**
     * @return bytes which are pending to be written
     */
    long pendingWrite();
}
