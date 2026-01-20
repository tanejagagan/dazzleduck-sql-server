package io.dazzleduck.sql.commons.ingestion;

public record QueueIdToTableMapping(String queueId, String tableName, String schemaName, String catalogName) {

}
