package io.dazzleduck.sql.commons.ingestion;

public interface IngestionTaskFactory {
    PostIngestionTask createPostIngestionTask(IngestionResult ingestionResult);

    String getTargetPath(String queueId);

    default String getTransformation(String queueId) {
        return null;
    }
}
