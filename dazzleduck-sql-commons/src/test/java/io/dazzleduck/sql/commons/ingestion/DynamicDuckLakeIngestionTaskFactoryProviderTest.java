package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DynamicDuckLakeIngestionTaskFactoryProviderTest {

    @TempDir
    Path tempDir;

    private DynamicDuckLakeIngestionTaskFactoryProvider providerFor(String dbPath) {
        var provider = new DynamicDuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(ConfigFactory.parseString(
                "db_path = \"" + dbPath + "\"\nconfig_load_interval_ms = 5000"));
        return provider;
    }

    private void initDb(String dbPath) throws Exception {
        try (SqliteQueueRepository repo = new SqliteQueueRepository(dbPath)) {
            repo.init();
        }
    }

    private void insertQueue(String dbPath, String queueId, String outputPath,
                             String catalog, String schema, String table,
                             String transformation) throws Exception {
        try (Connection rw = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement st = rw.createStatement()) {
            st.execute("INSERT INTO ingestion_queues " +
                       "(ingestion_queue, output_path, catalog, schema_name, table_name, transformation) " +
                       "VALUES ('" + queueId + "', '" + outputPath + "', '" +
                       catalog + "', '" + schema + "', '" + table + "', " +
                       (transformation == null ? "NULL" : "'" + transformation + "'") + ")");
        }
    }

    // -----------------------------------------------------------------------
    // loadMappings
    // -----------------------------------------------------------------------

    @Test
    void loadMappingsReturnsEmptyWhenConfigNotSet() {
        var provider = new DynamicDuckLakeIngestionTaskFactoryProvider();
        assertTrue(provider.loadMappings().isEmpty());
    }

    @Test
    void loadMappingsInitialisesNewDatabase() {
        String dbPath = tempDir.resolve("new.db").toString();
        var provider = providerFor(dbPath);
        // DB doesn't exist — loadMappings must create it and return empty map
        Map<String, QueueIdToTableMapping> mappings = provider.loadMappings();
        assertTrue(mappings.isEmpty());
    }

    @Test
    void loadMappingsReturnsInsertedRows() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs", "/data/logs", "loglake", "main", "logs", null);

        var mappings = providerFor(dbPath).loadMappings();

        assertEquals(1, mappings.size());
        QueueIdToTableMapping m = mappings.get("logs");
        assertNotNull(m);
        assertEquals("/data/logs", m.outputPath());
        assertEquals("loglake", m.catalog());
        assertEquals("main", m.schema());
        assertEquals("logs", m.table());
        assertNull(m.transformation());
    }

    @Test
    void loadMappingsReturnsMultipleRows() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs",    "/data/logs",    "loglake", "main", "logs",    null);
        insertQueue(dbPath, "traces",  "/data/traces",  "loglake", "main", "traces",  null);
        insertQueue(dbPath, "metrics", "/data/metrics", "loglake", "main", "metrics", null);

        assertEquals(3, providerFor(dbPath).loadMappings().size());
    }

    // -----------------------------------------------------------------------
    // getIngestionHandler
    // -----------------------------------------------------------------------

    @Test
    void getIngestionHandlerThrowsWhenConfigNotSet() {
        var provider = new DynamicDuckLakeIngestionTaskFactoryProvider();
        assertThrows(IllegalStateException.class, provider::getIngestionHandler);
    }

    @Test
    void getIngestionHandlerReturnsDynamicHandler() {
        String dbPath = tempDir.resolve("test.db").toString();
        IngestionHandler handler = providerFor(dbPath).getIngestionHandler();
        try {
            assertInstanceOf(DynamicIngestionHandler.class, handler);
        } finally {
            handler.closeQueues();
        }
    }

    @Test
    void getIngestionHandlerInitialisesSchemaOnNewDatabase() throws Exception {
        String dbPath = tempDir.resolve("new.db").toString();
        IngestionHandler handler = providerFor(dbPath).getIngestionHandler();
        try {
            // schema_version must exist and start at 0
            try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
                assertEquals(0L, SqliteQueueRepository.readSchemaVersion(conn));
            }
        } finally {
            handler.closeQueues();
        }
    }

    @Test
    void getIngestionHandlerLoadsInitialQueues() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs", "/data/logs", "loglake", "main", "logs", null);

        IngestionHandler handler = providerFor(dbPath).getIngestionHandler();
        try {
            assertEquals("/data/logs", handler.getTargetPath("logs"));
            assertTrue(handler.getKnownQueues().contains("logs"));
        } finally {
            handler.closeQueues();
        }
    }

    // -----------------------------------------------------------------------
    // validate
    // -----------------------------------------------------------------------

    @Test
    void validatePassesWithNoRows() {
        String dbPath = tempDir.resolve("test.db").toString();
        assertDoesNotThrow(() -> providerFor(dbPath).validate());
    }

    @Test
    void validatePassesWithValidTransformation() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs", "/data/logs", "loglake", "main", "logs",
                "SELECT id, ts FROM __this");

        assertDoesNotThrow(() -> providerFor(dbPath).validate());
    }

    @Test
    void validatePassesWithNoTransformation() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs", "/data/logs", "loglake", "main", "logs", null);

        assertDoesNotThrow(() -> providerFor(dbPath).validate());
    }

    @Test
    void validateFailsWhenTransformationDoesNotReferenceThis() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs", "/data/logs", "loglake", "main", "logs",
                "SELECT * FROM wrong_table");

        var ex = assertThrows(IllegalArgumentException.class,
                () -> providerFor(dbPath).validate());
        assertTrue(ex.getMessage().contains("__this"));
        assertTrue(ex.getMessage().contains("logs"));
    }

    @Test
    void validateChecksAllQueues() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        initDb(dbPath);
        insertQueue(dbPath, "logs",    "/data/logs",    "loglake", "main", "logs",    "SELECT * FROM __this");
        insertQueue(dbPath, "metrics", "/data/metrics", "loglake", "main", "metrics", "SELECT * FROM wrong_table");

        var ex = assertThrows(IllegalArgumentException.class,
                () -> providerFor(dbPath).validate());
        assertTrue(ex.getMessage().contains("metrics"));
    }
}
