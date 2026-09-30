package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.FlightRecorder;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class ResultSetStreamUtil {

    private static final Logger logger = LoggerFactory.getLogger(ResultSetStreamUtil.class);

    private ResultSetStreamUtil() {
        throw new UnsupportedOperationException("Utility class");
    }

    // How often a wait for a slow client re-checks the listener, in case a ready/cancel signal is missed.
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

        ReadyWaiter(FlightProducer.ServerStreamListener listener, Runnable onCancel) {
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

        /** Waits until the client can take another batch; false if the call was cancelled or the thread interrupted. */
        boolean awaitReady(FlightProducer.ServerStreamListener listener) {
            synchronized (lock) {
                while (!listener.isReady() && !listener.isCancelled()) {
                    try {
                        lock.wait(READY_RECHECK_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // e.g. the producer is shutting down
                        return false;
                    }
                }
            }
            return !listener.isCancelled();
        }
    }

    static void streamResultSet(ExecutorService executorService,
                                ResultSetSupplier supplier,
                                BufferAllocator allocator,
                                final int batchSize,
                                final FlightProducer.ServerStreamListener listener,
                                Runnable finalBlock,
                                FlightRecorder recorder) {
        executorService.submit(() -> {
            BufferAllocator childAllocator = null;
            var error = false;
            try {
                childAllocator = allocator.newChildAllocator("statement-allocator", 0, allocator.getLimit());
                recorder.startStream(false);
                var readiness = new ReadyWaiter(listener, () -> {});
                try (DuckDBResultSet resultSet = supplier.get();
                     ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(childAllocator, batchSize)) {
                    listener.start(reader.getVectorSchemaRoot());
                    while (!listener.isCancelled() && reader.loadNextBatch()) {
                        if (!readiness.awaitReady(listener)) {
                            break;
                        }
                        var size = childAllocator.getAllocatedMemory();
                        recorder.recordGetStream(false, size);
                        listener.putNext();
                    }
                }
            } catch (Throwable throwable) {
                error = true;
                recorder.errorStream(false);
                ErrorHandling.handleThrowable(listener, throwable);
            } finally {
                if (!error && !listener.isCancelled()) {
                    listener.completed();
                }
                recorder.endStream(false);
                finalBlock.run();
                if (childAllocator != null) {
                    childAllocator.close();
                }
            }
        });
    }

    static <T extends Statement> void streamResultSet(ExecutorService executorService,
                                                      StatementContext<T> statementContext,
                                                      DuckDBFlightSqlProducer.CacheKey key,
                                                      OptionalResultSetSupplier supplier,
                                                      BufferAllocator allocator,
                                                      final int batchSize,
                                                      final FlightProducer.ServerStreamListener listener,
                                                      Runnable finalBlock, FlightRecorder recorder) {

        executorService.submit(() -> {
            if (!statementContext.tryStart()) {
                // Another stream is running this statement. Reject without touching its state.
                listener.error(ErrorHandling.alreadyRunning());
                return;
            }
            BufferAllocator childAllocator = null;
            var error = false;
            try {
                childAllocator = allocator.newChildAllocator("statement-allocator", 0, allocator.getLimit());
                // A client that disconnects or cancels the DoGet must stop the query. Without this,
                // Flight drops every later putNext() silently and the query runs to completion.
                var readiness = new ReadyWaiter(listener, () -> {
                    try {
                        statementContext.cancel();
                    } catch (Exception e) {
                        logger.atDebug().setCause(e).log("Failed to cancel statement for a cancelled stream");
                    }
                });
                recorder.startStream(statementContext.isPreparedStatementContext());
                recorder.recordStatementStreamStart(key, statementContext);
                if (listener.isCancelled()) {
                    return; // cancelled before the handler was registered; finally still cleans up
                }
                supplier.execute();
                if (supplier.hasResultSet()) {
                    try (DuckDBResultSet resultSet = supplier.get();
                         ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(childAllocator, batchSize)) {
                        listener.start(reader.getVectorSchemaRoot());
                        while (!listener.isCancelled() && reader.loadNextBatch()) {
                            if (!readiness.awaitReady(listener)) {
                                break;
                            }
                            listener.putNext();
                            var size = childAllocator.getAllocatedMemory();
                            statementContext.bytesOut(size);
                            recorder.recordGetStream(statementContext.isPreparedStatementContext(),
                                    size);
                        }
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
                recorder.errorStream(statementContext.isPreparedStatementContext());
                recorder.recordStatementStreamError(key, statementContext, throwable);
                ErrorHandling.handleThrowable(listener, throwable);
            } finally {
                try {
                    if (!error && !listener.isCancelled()) {
                        listener.completed();
                    }
                    statementContext.end();
                    recorder.endStream(statementContext.isPreparedStatementContext());
                    recorder.recordStatementStreamEnd(key, statementContext);
                    finalBlock.run();
                    if (childAllocator != null) {
                        childAllocator.close();
                    }
                } catch (Exception e){
                    logger.atError().setCause(e).log("Error running finally block");
                }
            }
        });
    }

    static void streamResultSet(ExecutorService executorService,
                                ResultSetSupplierFromConnection supplier,
                                FlightProducer.CallContext context, AccessMode accessMode,
                                BufferAllocator allocator,
                                final FlightProducer.ServerStreamListener listener, FlightRecorder recorder) {

        streamResultSet(executorService, supplier, context, accessMode, allocator, listener, () -> {}, recorder);
    }

    static void streamResultSet(ExecutorService executorService,
                                        ResultSetSupplierFromConnection supplier,
                                        FlightProducer.CallContext context,
                                        AccessMode  accessMode,
                                        BufferAllocator allocator,
                                        final FlightProducer.ServerStreamListener listener,
                                        Runnable finalBlock,
                                        FlightRecorder recorder) {
        try {
            DuckDBConnection connection = DuckDBFlightSqlProducer.getConnection(context, accessMode );
            streamResultSet(executorService,
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
