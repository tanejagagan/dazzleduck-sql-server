package io.dazzleduck.sql.commons.ingestion;

public interface IngestionHandler {
    PostIngestionTask createPostIngestionTask(IngestionResult ingestionResult);

    String getTargetPath(String queueId);

    default String getTransformation(String queueId) {
        return null;
    }



    String[] getPartitionBy(String queueId);

    /**
     *
     * @return Indicate if partition-by-header is supported or not. If this is set to false and client sends partition by header
     * then this will result in error send to client indicating partition-by header is not supported
     *
     */
    default boolean supportPartitionByHeader(){
        return true;
    }
}
