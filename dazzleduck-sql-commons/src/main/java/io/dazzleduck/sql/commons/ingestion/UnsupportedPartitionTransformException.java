package io.dazzleduck.sql.commons.ingestion;

/**
 * A DuckLake table's partitioning cannot be reproduced by the COPY-based ingestion path — e.g.
 * {@code bucket(N)} on a floating-point column, whose IEEE-754 bit hashing has no exact SQL form.
 *
 * <p>Scoped to one queue: {@link DuckLakeIngestionHandler} records it in that queue's state so the
 * queue's writes fail with this message (rather than writing files under the wrong partition),
 * while every other queue keeps ingesting.
 */
public class UnsupportedPartitionTransformException extends IllegalStateException {

    public UnsupportedPartitionTransformException(String message) {
        super(message);
    }
}
