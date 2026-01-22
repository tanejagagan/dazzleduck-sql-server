package io.dazzleduck.sql.commons.ingestion;

public record QueueIdToTableMapping(String ingestionQueue, String catalog, String schema, String table) {

}
