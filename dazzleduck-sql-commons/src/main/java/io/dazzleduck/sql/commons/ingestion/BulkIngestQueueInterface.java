package io.dazzleduck.sql.commons.ingestion;


import java.time.Duration;
import java.util.concurrent.Future;

public interface BulkIngestQueueInterface<T, R> extends AutoCloseable, IngestionStatsMBean {

    Future<R> add(Batch<T> batch);

    void write(WriteTask<T, R> writeTask);

    /**
     * @return bytes which are pending to be written
     */
    long pendingWrite();

    /**
     * Gracefully flushes the queue: stops accepting new batches, finalizes the current bucket,
     * and blocks until every accepted batch has been written and its future completed (normally
     * or exceptionally by the write itself). Unlike {@link AutoCloseable#close()}, which abandons
     * buffered data and fails its futures, {@code drain} guarantees no accepted data is lost.
     *
     * <p>Idempotent and safe to call before {@link AutoCloseable#close()}. After it returns the
     * write thread has stopped, so any subsequent {@link #add(Batch)} is rejected.
     *
     * @throws InterruptedException if interrupted while waiting for pending writes to complete
     */
    void drain() throws InterruptedException;

    /**
     * Bounded variant of {@link #drain()} for shutdown paths that cannot block indefinitely on a
     * stalled write backend. Initiates the same flush, but waits at most {@code timeout} for it to
     * complete. A non-positive {@code timeout} initiates the flush without waiting.
     *
     * <p>Returns {@code false} if the writer is still running when the bound elapses — the caller
     * should then fall through to {@link AutoCloseable#close()}, which cancels the in-flight write
     * and releases resources.
     *
     * @param timeout maximum time to wait for pending writes to complete
     * @return {@code true} if all pending writes finished within {@code timeout}
     * @throws InterruptedException if interrupted while waiting
     */
    boolean drain(Duration timeout) throws InterruptedException;
}
