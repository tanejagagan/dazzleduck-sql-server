package io.dazzleduck.sql.commons.ingestion;

public class OutOfSequenceBatch extends Exception {
    public OutOfSequenceBatch(long current,  long produced) {
        super("Out of sequence batch current %s, produced %s".formatted(current, produced) );
    }
}
