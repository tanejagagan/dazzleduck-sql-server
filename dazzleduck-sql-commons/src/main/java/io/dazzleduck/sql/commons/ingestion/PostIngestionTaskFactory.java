package io.dazzleduck.sql.commons.ingestion;

public interface PostIngestionTaskFactory {
    PostIngestionTask create(IngestionResult ingestionResult);
}
