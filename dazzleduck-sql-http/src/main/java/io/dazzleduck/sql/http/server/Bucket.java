package io.dazzleduck.sql.http.server;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Bucket<T, R> {
    private final long capacity;
    private final List<Batch<T>> batches = new ArrayList<>();
    private final List<CompletableFuture<R>> futures = new ArrayList<>();
    private final Duration maxWriteDelay;
    private long size = 0;
    private Instant minReceiveInstance = Instant.MAX;
    private boolean scheduleWrite;

    public Bucket(long capacity, Duration maxWriteDelay) {
        this.capacity = capacity;
        this.maxWriteDelay = maxWriteDelay;
    }
    void add(Batch<T> batch, CompletableFuture<R> future) {
        batches.add(batch);
        futures.add(future);
        size += batch.totalSize();
        if (batch.receivedTime().isBefore(minReceiveInstance)) {
            minReceiveInstance = batch.receivedTime();
        }
    }

    public long size() {
        return size;
    }

    public List<Batch<T>> batches() {
        return batches;
    }

    public List<CompletableFuture<R>> futures() {
        return futures;
    }

    boolean isFull() {
        return size >= capacity;
    }

    public boolean timeExpired(Instant now) {
        return minReceiveInstance.plus(maxWriteDelay).isBefore(now);
    }

    public boolean readyForWrite(Instant now) {
        return !batches.isEmpty()
                && (minReceiveInstance.plus(maxWriteDelay).isBefore(now) || isFull());
    }

    public boolean isScheduleWrite(){
        return scheduleWrite;
    }

    public void writeScheduled() {
        scheduleWrite = true;
    }
}
