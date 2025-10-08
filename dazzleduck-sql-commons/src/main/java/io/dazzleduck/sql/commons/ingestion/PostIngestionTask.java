package io.dazzleduck.sql.commons.ingestion;

public interface PostIngestionTask {
    void execute();

    static PostIngestionTask NOOP = new PostIngestionTask() {
        @Override
        public void execute() {

        }
    };
}
