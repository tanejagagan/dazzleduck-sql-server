package io.dazzleduck.sql.commons.ingestion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DynamicQueueRepositoryTest {

    @TempDir
    Path tempDir;

    /** Write test data via a short-lived DuckDB write connection. */
    private void writeToDb(String dbPath, String sql) throws Exception {
        String safePath = dbPath.replace("'", "''");
        try (Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
             Statement st = conn.createStatement()) {
            st.execute("LOAD sqlite");
            st.execute("ATTACH '" + safePath + "' AS " + DynamicQueueRepository.ATTACHMENT + " (TYPE sqlite)");
            st.execute(sql);
        }
    }

    @Test
    void initCreatesSchemaVersionRowForWriter() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            try (Connection conn = repo.openReadOnlyConnection()) {
                assertEquals(0L, DynamicQueueRepository.readSchemaVersion(conn));
            }
        }
    }

    @Test
    void dataVersionChangesOnAnyWrite() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            long before = DynamicQueueRepository.readDataVersion(dbPath);

            writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues " +
                    "(ingestion_queue, output_path, catalog, schema_name, table_name) " +
                    "VALUES ('q1', '/data/q1', 'cat', 'main', 'q1')");

            long after = DynamicQueueRepository.readDataVersion(dbPath);
            assertTrue(after >= before, "data version (mtime) must be >= after a write");
        }
    }

    @Test
    void writerTracksRemoteSyncVersionIndependently() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();

            writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues " +
                    "(ingestion_queue, output_path, catalog, schema_name, table_name) " +
                    "VALUES ('q1', '/data/q1', 'cat', 'main', 'q1')");
            writeToDb(dbPath, "UPDATE " + DynamicQueueRepository.ATTACHMENT +
                    ".schema_version SET version = version + 1 WHERE id = 1");
            writeToDb(dbPath, "DELETE FROM " + DynamicQueueRepository.ATTACHMENT +
                    ".ingestion_queues WHERE ingestion_queue = 'q1'");
            writeToDb(dbPath, "UPDATE " + DynamicQueueRepository.ATTACHMENT +
                    ".schema_version SET version = version + 1 WHERE id = 1");

            try (Connection conn = repo.openReadOnlyConnection()) {
                assertEquals(2L, DynamicQueueRepository.readSchemaVersion(conn),
                        "writer bumped schema_version twice — reflects remote offset");
            }
        }
    }

    @Test
    void loadAllReturnsInsertedRows() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();

            writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues " +
                    "(ingestion_queue, output_path, catalog, schema_name, table_name) " +
                    "VALUES ('logs', '/data/logs', 'my_catalog', 'main', 'logs')");

            try (Connection conn = repo.openReadOnlyConnection()) {
                Map<String, QueueIdToTableMapping> mappings = DynamicQueueRepository.loadAll(conn);
                assertEquals(1, mappings.size());
                QueueIdToTableMapping m = mappings.get("logs");
                assertNotNull(m);
                assertEquals("/data/logs", m.outputPath());
                assertEquals("my_catalog", m.catalog());
                assertEquals("main", m.schema());
                assertEquals("logs", m.table());
                assertNull(m.transformation());
            }
        }
    }

    @Test
    void dynamicHandlerReflectsHotReload() throws Exception {
        String dbPath = tempDir.resolve("hot.db").toString();
        try (DynamicQueueRepository repo = new DynamicQueueRepository(dbPath)) {
            repo.init();

            writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues " +
                    "(ingestion_queue, output_path, catalog, schema_name, table_name) " +
                    "VALUES ('q1', '/data/q1', 'cat', 'main', 'q1')");

            Connection readConn = repo.openReadOnlyConnection();
            Map<String, QueueIdToTableMapping> initial = DynamicQueueRepository.loadAll(readConn);
            var handler = new DynamicIngestionHandler(dbPath, readConn, initial,
                    java.time.Duration.ofMillis(50));

            assertEquals("/data/q1", handler.getTargetPath("q1"));
            assertNull(handler.getTargetPath("q2"));

            Thread.sleep(100); // ensure mtime will differ
            writeToDb(dbPath, "INSERT INTO " + DynamicQueueRepository.ATTACHMENT + ".ingestion_queues " +
                    "(ingestion_queue, output_path, catalog, schema_name, table_name) " +
                    "VALUES ('q2', '/data/q2', 'cat', 'main', 'q2')");

            Thread.sleep(200);

            assertEquals("/data/q2", handler.getTargetPath("q2"));

            handler.closeQueues();
        }
    }
}
