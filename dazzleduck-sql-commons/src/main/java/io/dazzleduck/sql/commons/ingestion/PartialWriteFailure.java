package io.dazzleduck.sql.commons.ingestion;

import java.util.List;

/**
 * Thrown by a {@link BulkIngestQueueInterface#write} implementation that determined only SOME of
 * the flushed bucket's batches were affected by a failure, having already completed every batch's
 * future itself — successful ones normally, {@link #failedBatches()} exceptionally — before
 * throwing this.
 *
 * <p>{@link BulkIngestQueue#processWriteQueue} recognizes this type specially: it accounts for and
 * rolls back producer sequences for exactly {@link #failedBatches()}, recording the rest of the
 * bucket as successfully written. Any other exception from {@code write()} still marks the WHOLE
 * bucket as failed, as before this type existed — {@code write()} implementations that cannot
 * attribute a failure to specific batches should keep throwing a plain exception.
 */
public final class PartialWriteFailure extends RuntimeException {

    private final List<Batch<?>> failedBatches;

    public PartialWriteFailure(String message, Throwable cause, List<Batch<?>> failedBatches) {
        super(message, cause);
        this.failedBatches = List.copyOf(failedBatches);
    }

    /** The subset of the flushed bucket's batches whose futures were completed exceptionally. */
    public List<Batch<?>> failedBatches() {
        return failedBatches;
    }
}
