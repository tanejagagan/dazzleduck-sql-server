package io.dazzleduck.sql.flight.server;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/**
 * Where result-stream work runs.
 *
 * <p>A stream spends most of its life waiting: for its client to take the next batch (backpressure)
 * or for DuckDB. It runs on a virtual thread ({@link #streams}), so a stalled client parks a nearly
 * free virtual thread instead of holding one of a few platform threads; a handful of idle clients
 * used to occupy the whole fixed pool and queue every other query. {@code ReadyWaiter}'s
 * {@code synchronized}/{@code wait()} no longer pin a virtual thread since JDK 24 (JEP 491).
 *
 * <p>Every DuckDB call is native, and a virtual thread inside a native call pins its carrier until
 * the call returns; carriers are shared by every virtual thread in the JVM (including Helidon's HTTP
 * handlers), so a few long queries on virtual threads would freeze all of them. DuckDB calls
 * therefore go through {@link #duckdb(Callable)}, which runs them on the bounded platform pool
 * ({@link #duckdb}) while the virtual thread parks on the result. That pool is also what bounds how
 * many DuckDB operations run at once.
 */
public final class StreamExecutors {

    private final ExecutorService streams;
    private final ExecutorService duckdb;

    private StreamExecutors(ExecutorService streams, ExecutorService duckdb) {
        this.streams = streams;
        this.duckdb = duckdb;
    }

    /** Virtual threads for stream tasks, and {@code duckdbPool} (platform threads) for DuckDB calls. */
    public static StreamExecutors create(ExecutorService duckdbPool) {
        return new StreamExecutors(
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("flight-stream-", 0).factory()),
                duckdbPool);
    }

    /** For tests: stream tasks on {@code streams}, DuckDB calls inline on the stream's own thread. */
    static StreamExecutors sameThread(ExecutorService streams) {
        return new StreamExecutors(streams, null);
    }

    /** Submits a stream task. Throws {@link RejectedExecutionException} if shutting down. */
    void submit(Runnable task) {
        streams.submit(task);
    }

    /**
     * Runs a DuckDB call on the platform pool and waits for it (a virtual thread parks meanwhile).
     * The call's own exception is rethrown as is; if the pool is shutting down, it runs inline.
     */
    <V> V duckdb(Callable<V> call) throws Exception {
        if (duckdb == null) {
            return call.call();
        }
        java.util.concurrent.Future<V> result;
        try {
            result = duckdb.submit(call);
        } catch (RejectedExecutionException shuttingDown) {
            return call.call();
        }
        try {
            return result.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw e;
        }
    }

    /** {@link #duckdb(Callable)} for a call without a result. */
    void duckdbRun(ThrowingRunnable call) throws Exception {
        duckdb(() -> {
            call.run();
            return null;
        });
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }

    ExecutorService streams() {
        return streams;
    }
}
