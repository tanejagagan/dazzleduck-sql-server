package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.FlightRecorder;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BooleanSupplier;

public class ResultSetStreamUtil {

    private static final Logger logger = LoggerFactory.getLogger(ResultSetStreamUtil.class);

    private ResultSetStreamUtil() {
        throw new UnsupportedOperationException("Utility class");
    }

    // How often a wait for a slow client re-checks the listener (in case a ready/cancel signal is
    // missed) and whether the query was cancelled server-side.
    private static final long READY_RECHECK_MS = 1_000;

    /**
     * Makes a stream wait for its client. Without it, putNext() hands every batch to gRPC as fast
     * as DuckDB produces it, and a slow reader makes the server buffer the whole result in direct
     * memory. Also runs {@code onCancel} (interrupting the query) when the call is cancelled: a
     * client that disconnects or cancels must stop the query, not only the stream.
     *
     * <p>Not Arrow's CallbackBackpressureStrategy: that requires setOnReadyHandler, which the HTTP
     * listeners do not implement (Arrow's default throws). They write synchronously to the response,
     * so the blocking write is already their backpressure and they are ready once started.
     */
    static final class ReadyWaiter {
        private final Object lock = new Object();
        private final BooleanSupplier cancelRequested;

        /**
         * @param cancelRequested whether the query was cancelled server-side (CancelFlightInfo, or
         *     close() cancelling running queries). Polled on each recheck rather than signalled:
         *     the statement's cancel() holds its context's monitor, and calling into this waiter's
         *     lock from there while the waiter checks the context under its lock would deadlock.
         */
        ReadyWaiter(FlightProducer.ServerStreamListener listener, Runnable onCancel, BooleanSupplier cancelRequested) {
            this.cancelRequested = cancelRequested;
            try {
                listener.setOnReadyHandler(this::signal);
            } catch (UnsupportedOperationException notSupported) {
                // HTTP listeners: no transport buffer to wait for.
            }
            listener.setOnCancelHandler(() -> {
                try {
                    onCancel.run();
                } finally {
                    signal();
                }
            });
        }

        private void signal() {
            synchronized (lock) {
                lock.notifyAll();
            }
        }

        /**
         * Waits until the client can take another batch. Returns false if the client went away (the
         * stream just stops; there is no one to tell). Throws if the stream must end with an error
         * instead: the query was cancelled server-side (CANCELLED), or this thread was interrupted
         * because the producer is shutting down (UNAVAILABLE). Breaking out normally there would
         * let the stream call completed() and report a cut-off result as complete.
         */
        boolean awaitReady(FlightProducer.ServerStreamListener listener) {
            synchronized (lock) {
                while (!listener.isReady() && !listener.isCancelled()) {
                    if (cancelRequested.getAsBoolean()) {
                        throw CallStatus.CANCELLED.withDescription("Query was cancelled").toRuntimeException();
                    }
                    try {
                        lock.wait(READY_RECHECK_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw CallStatus.UNAVAILABLE.withDescription("Server is shutting down").toRuntimeException();
                    }
                }
            }
            return !listener.isCancelled();
        }
    }

    /**
     * Submits a stream task whose {@code finalBlock} releases what the stream holds (a connection,
     * a cursor entry). If the executor rejects the task (shutting down), the task never runs, so
     * {@code finalBlock} runs here instead; the rejection is rethrown for the caller to report.
     */
    private static void submit(StreamExecutors executors, Runnable finalBlock, Runnable task) {
        try {
            executors.submit(task);
        } catch (RejectedExecutionException e) {
            finalBlock.run();
            throw e;
        }
    }

    /** Runs {@code finalBlock} (it closes DuckDB resources) on the fetch pool; logs, never throws. */
    private static void runFinalBlock(StreamExecutors executors, Runnable finalBlock) {
        try {
            executors.cleanup(finalBlock::run);
        } catch (Exception e) {
            logger.atError().setCause(e).log("Error running a stream's final block");
        }
    }

    /**
     * An HTTP response whose write failed for a reason other than its client going away (e.g. a
     * serialization error), or null. The stream stopped on it like on a disconnect, but it is a
     * server-side failure and must still count as one.
     */
    private static Throwable serverWriteFailure(FlightProducer.ServerStreamListener listener) {
        if (listener instanceof HttpResponseListener response) {
            Throwable failure = response.writeFailure();
            if (failure != null && !HttpResponseListener.isClientGone(failure)) {
                return failure;
            }
        }
        return null;
    }

    /** Closes the Arrow reader, then the result set, on the fetch pool (both are native). */
    private static void closeOnDuckDb(StreamExecutors executors, ArrowReader reader, DuckDBResultSet resultSet)
            throws Exception {
        executors.cleanup(() -> {
            try {
                if (reader != null) {
                    reader.close();
                }
            } finally {
                if (resultSet != null) {
                    resultSet.close();
                }
            }
        });
    }

    /**
     * Starts {@code listener} on {@code batches}, which is also the stream's DictionaryProvider: a
     * dictionary-encoded column (DuckDB sends ENUM that way) cannot be written without it. Flight
     * sends the dictionaries once, from start(), and DuckDB fills them in while loading a batch, so
     * with such a column the first batch is loaded before starting. Other results start right away.
     *
     * @return whether a batch is already loaded, to be sent before loading the next
     */
    private static boolean start(FlightProducer.ServerStreamListener listener, ArrowReader batches,
                                 StreamExecutors executors) throws Exception {
        VectorSchemaRoot root = batches.getVectorSchemaRoot();
        boolean loaded = hasDictionary(root.getSchema().getFields()) && executors.fetch(batches::loadNextBatch);
        listener.start(root, batches);
        return loaded;
    }

    private static boolean hasDictionary(List<Field> fields) {
        for (Field field : fields) {
            if (field.getDictionary() != null || hasDictionary(field.getChildren())) {
                return true;
            }
        }
        return false;
    }

    static void streamResultSet(StreamExecutors executors,
                                ResultSetSupplier supplier,
                                BufferAllocator allocator,
                                final int batchSize,
                                final FlightProducer.ServerStreamListener listener,
                                Runnable finalBlock,
                                FlightRecorder recorder) {
        submit(executors, finalBlock, () -> {
            BufferAllocator childAllocator = null;
            var error = false;
            try {
                childAllocator = allocator.newChildAllocator("statement-allocator", 0, allocator.getLimit());
                final BufferAllocator streamAllocator = childAllocator;
                recorder.startStream(false);
                var readiness = new ReadyWaiter(listener, () -> {}, () -> false);
                DuckDBResultSet resultSet = null;
                ArrowReader reader = null;
                try {
                    resultSet = executors.execute(supplier::get); // runs the metadata query
                    final DuckDBResultSet rs = resultSet;
                    reader = executors.fetch(() -> (ArrowReader) rs.arrowExportStream(streamAllocator, batchSize));
                    final ArrowReader batches = reader;
                    boolean loaded = start(listener, batches, executors);
                    while (!listener.isCancelled() && (loaded || executors.fetch(batches::loadNextBatch))) {
                        loaded = false;
                        if (!readiness.awaitReady(listener)) {
                            break;
                        }
                        var size = childAllocator.getAllocatedMemory();
                        recorder.recordGetStream(false, size);
                        listener.putNext();
                    }
                } finally {
                    closeOnDuckDb(executors, reader, resultSet);
                }
            } catch (Throwable throwable) {
                error = true;
                recorder.errorStream(false);
                ErrorHandling.handleThrowable(listener, throwable);
            } finally {
                if (!error && !listener.isCancelled()) {
                    listener.completed();
                }
                Throwable writeFailure = error ? null : serverWriteFailure(listener);
                if (writeFailure != null) {
                    recorder.errorStream(false); // the listener has logged it
                }
                recorder.endStream(false);
                runFinalBlock(executors, finalBlock);
                if (childAllocator != null) {
                    childAllocator.close();
                }
            }
        });
    }

    static <T extends Statement> void streamResultSet(StreamExecutors executors,
                                                      StatementContext<T> statementContext,
                                                      DuckDBFlightSqlProducer.CacheKey key,
                                                      OptionalResultSetSupplier supplier,
                                                      BufferAllocator allocator,
                                                      final int batchSize,
                                                      final FlightProducer.ServerStreamListener listener,
                                                      Runnable finalBlock, FlightRecorder recorder) {

        submit(executors, finalBlock, () -> {
            if (!statementContext.tryStart()) {
                // Another stream is running this statement, or it was closed. Reject without touching
                // its state (no end()), but still run this stream's own cleanup: for a plain
                // statement, whose context can only fail here once closed, that removes the closed
                // entry from the cursor cache instead of leaving it for the TTL.
                listener.error(ErrorHandling.cannotStart(statementContext));
                runFinalBlock(executors, finalBlock);
                return;
            }
            BufferAllocator childAllocator = null;
            var error = false;
            try {
                childAllocator = allocator.newChildAllocator("statement-allocator", 0, allocator.getLimit());
                final BufferAllocator streamAllocator = childAllocator;
                // A client that disconnects or cancels the DoGet must stop the query. Without this,
                // Flight drops every later putNext() silently and the query runs to completion.
                var readiness = new ReadyWaiter(listener, () -> {
                    try {
                        statementContext.cancel();
                    } catch (Exception e) {
                        logger.atDebug().setCause(e).log("Failed to cancel statement for a cancelled stream");
                    }
                }, statementContext::isCancelRequested);
                recorder.startStream(statementContext.isPreparedStatementContext());
                recorder.recordStatementStreamStart(key, statementContext);
                if (listener.isCancelled()) {
                    return; // cancelled before the handler was registered; finally still cleans up
                }
                if (statementContext.isCancelRequested()) {
                    // CancelFlightInfo reached it while it was queued: end without running it.
                    error = true;
                    listener.error(CallStatus.CANCELLED.withDescription("Query was cancelled").toRuntimeException());
                    return;
                }
                executors.execute(() -> {
                    supplier.execute();
                    return null;
                });
                if (supplier.hasResultSet()) {
                    DuckDBResultSet resultSet = null;
                    ArrowReader reader = null;
                    try {
                        resultSet = executors.fetch(supplier::get);
                        final DuckDBResultSet rs = resultSet;
                        reader = executors.fetch(() -> (ArrowReader) rs.arrowExportStream(streamAllocator, batchSize));
                        final ArrowReader batches = reader;
                        boolean loaded = start(listener, batches, executors);
                        while (!listener.isCancelled() && (loaded || executors.fetch(batches::loadNextBatch))) {
                            loaded = false;
                            if (!readiness.awaitReady(listener)) {
                                break;
                            }
                            listener.putNext();
                            var size = childAllocator.getAllocatedMemory();
                            statementContext.bytesOut(size);
                            recorder.recordGetStream(statementContext.isPreparedStatementContext(),
                                    size);
                        }
                    } finally {
                        closeOnDuckDb(executors, reader, resultSet);
                    }
                } else {
                    listener.start(new VectorSchemaRoot(List.of()));
                }
            } catch (Throwable throwable) {
                error = true;
                if (listener.isCancelled()) {
                    // The caller went away and we interrupted the query: not a query error.
                    logger.atDebug().setCause(throwable).log("Stream ended after the caller cancelled");
                    return;
                }
                if (statementContext.isCancelRequested()) {
                    // Interrupted by CancelFlightInfo while the client is still connected: tell it the
                    // query was cancelled, and don't count it as a query error.
                    logger.atDebug().setCause(throwable).log("Stream ended after a server-side cancel");
                    listener.error(CallStatus.CANCELLED.withDescription("Query was cancelled").toRuntimeException());
                    return;
                }
                recorder.errorStream(statementContext.isPreparedStatementContext());
                recorder.recordStatementStreamError(key, statementContext, throwable);
                ErrorHandling.handleThrowable(listener, throwable);
            } finally {
                try {
                    if (!error && !listener.isCancelled()) {
                        listener.completed();
                    }
                    Throwable writeFailure = error ? null : serverWriteFailure(listener);
                    if (writeFailure != null) {
                        recorder.errorStream(statementContext.isPreparedStatementContext());
                        recorder.recordStatementStreamError(key, statementContext, writeFailure);
                    }
                    statementContext.end();
                    recorder.endStream(statementContext.isPreparedStatementContext());
                    recorder.recordStatementStreamEnd(key, statementContext);
                    runFinalBlock(executors, finalBlock);
                    if (childAllocator != null) {
                        childAllocator.close();
                    }
                } catch (Exception e){
                    logger.atError().setCause(e).log("Error running finally block");
                }
            }
        });
    }

    static void streamResultSet(StreamExecutors executors,
                                ResultSetSupplierFromConnection supplier,
                                FlightProducer.CallContext context, AccessMode accessMode,
                                BufferAllocator allocator,
                                final FlightProducer.ServerStreamListener listener, FlightRecorder recorder) {

        streamResultSet(executors, supplier, context, accessMode, allocator, listener, () -> {}, recorder);
    }

    static void streamResultSet(StreamExecutors executors,
                                        ResultSetSupplierFromConnection supplier,
                                        FlightProducer.CallContext context,
                                        AccessMode  accessMode,
                                        BufferAllocator allocator,
                                        final FlightProducer.ServerStreamListener listener,
                                        Runnable finalBlock,
                                        FlightRecorder recorder) {
        try {
            DuckDBConnection connection = DuckDBFlightSqlProducer.getConnection(context, accessMode );
            streamResultSet(executors,
                    () -> supplier.get(connection),
                    allocator,
                    DuckDBFlightSqlProducer.getBatchSize(context),
                    listener,
                    () -> {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            logger.atError().setCause(e).log("Error closing connection");
                        }
                        finalBlock.run();
                    }, recorder);
        } catch (Throwable t) {
            ErrorHandling.handleThrowable(listener, t);
        }
    }
}
