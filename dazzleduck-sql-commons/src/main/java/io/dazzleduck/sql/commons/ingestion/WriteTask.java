package io.dazzleduck.sql.commons.ingestion;

import java.time.Instant;
import java.util.Objects;

public final class WriteTask<T, R> {
    private final long taskId;
    private final Instant startTime;
    private final Bucket<T, R> bucket;

    private Runnable cancelHook;
    private boolean cancel = false;

    public WriteTask(long taskId, Instant startTime, Bucket<T, R> bucket) {
        this.taskId = taskId;
        this.startTime = startTime;
        this.bucket = bucket;
    }


    public long size() {
        return bucket.size();
    }

    public synchronized void cancel() {
        this.cancel = true;
        if (cancelHook != null) {
            try {
                cancelHook.run();
            } catch (Exception ignored){
                //  Eat the exception
            }
        }
    }

    public synchronized boolean setCancelHook(Runnable runnable){
        if(!cancel) {
            cancelHook = runnable;
            return true;
        }
        return false;
    }

    public synchronized boolean isCancel(){
        return this.cancel;
    }

    public long taskId() {
        return taskId;
    }

    public Instant startTime() {
        return startTime;
    }

    public Bucket<T, R> bucket() {
        return bucket;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (WriteTask) obj;
        return this.taskId == that.taskId &&
                Objects.equals(this.startTime, that.startTime) &&
                Objects.equals(this.bucket, that.bucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, startTime, bucket);
    }

    @Override
    public String toString() {
        return "WriteTask[" +
                "taskId=" + taskId + ", " +
                "startTime=" + startTime + ", " +
                "bucket=" + bucket + ']';
    }

}
