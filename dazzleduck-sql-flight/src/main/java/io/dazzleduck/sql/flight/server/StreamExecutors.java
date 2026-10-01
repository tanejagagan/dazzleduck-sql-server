package io.dazzleduck.sql.flight.server;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
 * handlers), so a few long queries on virtual threads would freeze all of them. DuckDB calls therefore
 * run on bounded platform pools while the virtual thread waits for them:
 * <ul>
 *   <li>{@link #execute}: starting a query (the call that can take as long as the query plans and,
 *       for a non-streaming result, runs);</li>
 *   <li>{@link #fetch}: everything a running stream needs next (the result set, the Arrow export,
 *       each batch) and its cleanup. A separate pool, so streams already running are never queued
 *       behind new long executes.</li>
 * </ul>
 * The pools also bound how many DuckDB operations run at once.
 */
public final class StreamExecutors {

    private final ExecutorService streams;
    private final ExecutorService execute;
    private final ExecutorService fetch;

    private StreamExecutors(ExecutorService streams, ExecutorService execute, ExecutorService fetch) {
        this.streams = streams;
        this.execute = execute;
        this.fetch = fetch;
    }

    /** Virtual threads for stream tasks; platform pools for starting queries and for fetching from them. */
    public static StreamExecutors create(ExecutorService executePool, ExecutorService fetchPool) {
        return new StreamExecutors(
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("flight-stream-", 0).factory()),
                executePool, fetchPool);
    }

    /** For tests: stream tasks on {@code streams}, DuckDB calls inline on the stream's own thread. */
    static StreamExecutors sameThread(ExecutorService streams) {
        return new StreamExecutors(streams, null, null);
    }

    /** Submits a stream task. Throws {@link RejectedExecutionException} if shutting down. */
    void submit(Runnable task) {
        streams.submit(task);
    }

    /** Starts a query on the execute pool and waits for it. Rejected (shutting down) is rethrown. */
    <V> V execute(Callable<V> call) throws Exception {
        return runOn(execute, call, false);
    }

    /** Fetches from a running query on the fetch pool and waits. Rejected (shutting down) is rethrown. */
    <V> V fetch(Callable<V> call) throws Exception {
        return runOn(fetch, call, false);
    }

    /**
     * Closes DuckDB resources on the fetch pool and waits. If the pool is already shut down, runs
     * inline instead: briefly pinning a carrier during shutdown is better than leaking the native
     * result or connection.
     */
    void cleanup(ThrowingRunnable call) throws Exception {
        runOn(fetch, () -> {
            call.run();
            return null;
        }, true);
    }

    private static <V> V runOn(ExecutorService pool, Callable<V> call, boolean inlineIfRejected) throws Exception {
        if (pool == null) {
            return call.call();
        }
        Future<V> result;
        try {
            result = pool.submit(call);
        } catch (RejectedExecutionException shuttingDown) {
            if (inlineIfRejected) {
                return call.call();
            }
            throw shuttingDown;
        }
        // Once submitted, wait for the call to finish even if this thread is interrupted (e.g. by
        // close()'s shutdownNow): the native call keeps running on the pool, and returning early would
        // let the stream close the result set or reader while DuckDB is still using it. The wait is
        // bounded because close() cancels running queries first, and a call the pool drops at shutdown is
    // cancelled (shutdownNowAndCancel), which ends the wait. The interrupt is restored after.
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return result.get();
                } catch (InterruptedException e) {
                    interrupted = true;
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
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * A fixed pool of {@code size} platform threads named {@code name-N}, for DuckDB calls (see the
     * class comment).
     */
    public static ExecutorService duckDbPool(String name, int size) {
        return Executors.newFixedThreadPool(size, Thread.ofPlatform().name(name + "-", 0).factory());
    }

    /**
     * {@link ExecutorService#shutdownNow()}, and also cancels the queued calls it drops. A dropped
     * call's future would otherwise never complete, and since the stream waits for its call even if
     * interrupted, that stream would wait forever: it would never end, close its resources or give
     * back its connection. Cancelled, its wait ends with a {@link java.util.concurrent.CancellationException}.
     */
    public static void shutdownNowAndCancel(ExecutorService pool) {
        for (Runnable dropped : pool.shutdownNow()) {
            if (dropped instanceof Future<?> call) {
                call.cancel(false);
            }
        }
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }

    ExecutorService streams() {
        return streams;
    }
}
