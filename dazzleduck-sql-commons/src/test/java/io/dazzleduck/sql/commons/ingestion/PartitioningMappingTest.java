package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the {@code partition_column}/{@code parallel_writers} queue-mapping fields: record
 * validation, defaults, and provider config parsing (mirrors {@link ExtractClaimsMappingTest}).
 */
class PartitioningMappingTest {

    @TempDir
    Path tempDir;

    private static final String CATALOG = "partitioning_mapping_lake";

    @BeforeAll
    static void loadExtensions() throws Exception {
        ConnectionPool.executeBatch(new String[]{"INSTALL ducklake", "LOAD ducklake"});
    }

    @AfterEach
    void detach() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH DATABASE IF EXISTS " + CATALOG);
        }
    }

    @Test
    void mapping_defaultsToSingleWriterAndNoPartitionColumn() {
        var mapping = new QueueIdToTableMapping("q", "cat", "main", "t", Map.of(), null);
        assertNull(mapping.partitionColumn());
        assertEquals(1, mapping.parallelWriters());
    }

    @Test
    void withPartitioning_setsBothFieldsAndPreservesOtherState() {
        var mapping = new QueueIdToTableMapping("q", "cat", "main", "t", Map.of(), null)
                .withExtractClaims(true)
                .withPartitioning("id", 4);

        assertEquals("id", mapping.partitionColumn());
        assertEquals(4, mapping.parallelWriters());
        assertTrue(mapping.extractClaims(), "withPartitioning must preserve unrelated fields");
    }

    @Test
    void parallelWritersGreaterThanOneRequiresPartitionColumn() {
        var e = assertThrows(IllegalArgumentException.class, () ->
                new QueueIdToTableMapping("q", "cat", "main", "t", Map.of(), null).withPartitioning(null, 4));
        assertTrue(e.getMessage().contains("parallel_writers"), e.getMessage());
    }

    @Test
    void partitionColumnWithoutParallelWritersGreaterThanOneIsRejected() {
        var e = assertThrows(IllegalArgumentException.class, () ->
                new QueueIdToTableMapping("q", "cat", "main", "t", Map.of(), null).withPartitioning("id", 1));
        assertTrue(e.getMessage().contains("partition_column"), e.getMessage());
    }

    @Test
    void parallelWritersBelowOneIsRejected() {
        assertThrows(IllegalArgumentException.class, () ->
                new QueueIdToTableMapping("q", "cat", "main", "t", Map.of(), null).withPartitioning("id", 0));
    }

    @Test
    void provider_parsesPartitioningPerMapping() {
        var config = ConfigFactory.parseString("""
                ingestion_queue_table_mapping = [
                    { ingestion_queue = "sharded", catalog = "c", schema = "s", table = "t1", partition_column = "id", parallel_writers = 4 }
                    { ingestion_queue = "plain",   catalog = "c", schema = "s", table = "t2" }
                ]
                """);
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(config);
        Map<String, QueueIdToTableMapping> mappings = provider.loadMappings();

        var sharded = mappings.get("sharded");
        assertEquals("id", sharded.partitionColumn());
        assertEquals(4, sharded.parallelWriters());

        var plain = mappings.get("plain");
        assertNull(plain.partitionColumn());
        assertEquals(1, plain.parallelWriters());
    }

    @Test
    void handler_looksUpPartitioningPerQueue() throws Exception {
        Path dataPath = tempDir.resolve("data");
        Files.createDirectories(dataPath);
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')"
                            .formatted(tempDir.resolve("catalog"), CATALOG, dataPath),
                    "CREATE TABLE %s.main.events (id BIGINT)".formatted(CATALOG)
            });
        }

        var mapping = new QueueIdToTableMapping("q", CATALOG, "main", "events", Map.of(), null)
                .withPartitioning("id", 4);
        var handler = new DuckLakeIngestionHandler(Map.of("q", mapping));

        assertEquals("id", handler.getPartitionColumn("q"));
        assertEquals(4, handler.getParallelWriters("q"));
        assertNull(handler.getPartitionColumn("unknown-queue"));
        assertEquals(1, handler.getParallelWriters("unknown-queue"));
    }
}
