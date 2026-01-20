package io.dazzleduck.sql.commons.ingestion;

public interface IngestionTaskFactory {
    PostIngestionTask createPostIngestionTask(IngestionResult ingestionResult);

    String getTargetPath(String queueId);
}
