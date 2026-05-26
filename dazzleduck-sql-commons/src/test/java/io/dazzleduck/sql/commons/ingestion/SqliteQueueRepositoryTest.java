package io.dazzleduck.sql.commons.ingestion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SqliteQueueRepositoryTest {

    @TempDir
    Path tempDir;

    @Test
    void initCreatesSchemaVersionRowForWriter() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        try (SqliteQueueRepository repo = new SqliteQueueRepository(dbPath)) {
            repo.init();
            try (Connection conn = repo.openReadOnlyConnection()) {
                // schema_version starts at 0 — writer uses this for remote-sync bookkeeping
                assertEquals(0L, SqliteQueueRepository.readSchemaVersion(conn));
            }
        }
    }

    @Test
    void dataVersionChangesOnAnyWrite() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        try (SqliteQueueRepository repo = new SqliteQueueRepository(dbPath)) {
            repo.init();
            try (Connection readConn = repo.openReadOnlyConnection()) {
                long before = SqliteQueueRepository.readDataVersion(readConn);

                try (Connection rw = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                     Statement st = rw.createStatement()) {
                    st.execute("INSERT INTO ingestion_queues (ingestion_queue, output_path, catalog, schema_name, table_name) " +
                               "VALUES ('q1', '/data/q1', 'cat', 'main', 'q1')");
                }

                long after = SqliteQueueRepository.readDataVersion(readConn);
                assertTrue(after > before, "data_version must increase after a write");
            }
        }
    }

    @Test
    void writerTracksRemoteSyncVersionIndependently() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        try (SqliteQueueRepository repo = new SqliteQueueRepository(dbPath)) {
            repo.init();

            // Writer bumps schema_version atomically with queue changes to track remote offset
            try (Connection rw = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
                rw.setAutoCommit(false);
                try (Statement st = rw.createStatement()) {
                    st.execute("INSERT INTO ingestion_queues (ingestion_queue, output_path, catalog, schema_name, table_name) " +
                               "VALUES ('q1', '/data/q1', 'cat', 'main', 'q1')");
                    st.execute("UPDATE schema_version SET version = version + 1 WHERE id = 1");
                }
                rw.commit();

                rw.setAutoCommit(false);
                try (Statement st = rw.createStatement()) {
                    st.execute("DELETE FROM ingestion_queues WHERE ingestion_queue = 'q1'");
                    st.execute("UPDATE schema_version SET version = version + 1 WHERE id = 1");
                }
                rw.commit();
            }

            try (Connection conn = repo.openReadOnlyConnection()) {
                assertEquals(2L, SqliteQueueRepository.readSchemaVersion(conn),
                        "writer bumped schema_version twice — reflects remote offset");
            }
        }
    }

    @Test
    void loadAllReturnsInsertedRows() throws Exception {
        String dbPath = tempDir.resolve("test.db").toString();
        try (SqliteQueueRepository repo = new SqliteQueueRepository(dbPath)) {
            repo.init();

            try (Connection rw = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement st = rw.createStatement()) {
                st.execute("INSERT INTO ingestion_queues (ingestion_queue, output_path, catalog, schema_name, table_name) " +
                           "VALUES ('logs', '/data/logs', 'my_catalog', 'main', 'logs')");
            }

            try (Connection conn = repo.openReadOnlyConnection()) {
                Map<String, QueueIdToTableMapping> mappings = SqliteQueueRepository.loadAll(conn);
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
        try (SqliteQueueRepository repo = new SqliteQueueRepository(dbPath)) {
            repo.init();

            // Insert initial row — data_version will change automatically
            try (Connection rw = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement st = rw.createStatement()) {
                st.execute("INSERT INTO ingestion_queues (ingestion_queue, output_path, catalog, schema_name, table_name) " +
                           "VALUES ('q1', '/data/q1', 'cat', 'main', 'q1')");
            }

            Connection readConn = repo.openReadOnlyConnection();
            Map<String, QueueIdToTableMapping> initial = SqliteQueueRepository.loadAll(readConn);
            var handler = new DynamicIngestionHandler(readConn, initial, java.time.Duration.ofMillis(50));

            assertEquals("/data/q1", handler.getTargetPath("q1"));
            assertNull(handler.getTargetPath("q2"));

            // Insert a new row — no manual version bump needed; data_version handles detection
            try (Connection rw = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
                 Statement st = rw.createStatement()) {
                st.execute("INSERT INTO ingestion_queues (ingestion_queue, output_path, catalog, schema_name, table_name) " +
                           "VALUES ('q2', '/data/q2', 'cat', 'main', 'q2')");
            }

            Thread.sleep(200);

            assertEquals("/data/q2", handler.getTargetPath("q2"));

            handler.closeQueues();
        }
    }
}
