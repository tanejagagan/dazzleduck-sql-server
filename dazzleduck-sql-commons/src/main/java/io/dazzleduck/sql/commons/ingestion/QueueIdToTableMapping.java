package io.dazzleduck.sql.commons.ingestion;

import java.util.Map;

public record QueueIdToTableMapping(String ingestionQueue, String catalog, String schema, String table, Map<String, String> additionalParameters) {

}
