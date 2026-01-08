package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static io.dazzleduck.sql.http.server.ContentTypes.APPLICATION_ARROW;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for DuckLake post-ingestion task.
 * Tests the complete flow: ingest -> post-ingestion task -> verify file data in DuckLake.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class HttpServerDuckLakePostIngestionTest {
    private static HttpClient client;

    @TempDir
    private static Path warehousePath;

    @TempDir
    private static Path tempDir;

    private static String detectedDatabase;
    private static String detectedMetadataDatabase;
    private static String ducklakeCatalogPath;
    private static String ducklakeDataPath;
    private static Long tableId;

    private static final int TEST_PORT = 8096;
    private static final String BASE_URL = "http://localhost:" + TEST_PORT;
    private static final String DUCKLAKE_CATALOG = "test_ducklake";
    private static final String DUCKLAKE_METADATA = "__ducklake_metadata_" + DUCKLAKE_CATALOG;
    private static final String TEST_TABLE = "test_logs";
    private static final String INGEST_PATH = "logs";

    @BeforeAll
    public static void setup() throws Exception {
        client = HttpClient.newHttpClient();

        ducklakeCatalogPath = tempDir.resolve(DUCKLAKE_CATALOG).toString();
        ducklakeDataPath = tempDir.resolve("ducklake_data").toString();
        // Create ingestion directory
        Files.createDirectories(warehousePath.resolve(INGEST_PATH));
        // Initialize DuckLake
        setupDuckLake();
        // Start HTTP server with DuckLake configuration
        startServer();
        // Wait for server to start
        Thread.sleep(2000);
    }

    private static void setupDuckLake() throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "INSTALL arrow",
                    "LOAD arrow"
            });
            // Install and load DuckLake
            String[] setupQueries = {
                    "LOAD ducklake",
                    String.format("ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')", ducklakeCatalogPath, DUCKLAKE_CATALOG, ducklakeDataPath),
                    "USE " + DUCKLAKE_CATALOG,
                    // Create test table with the schema matching our test data
                    String.format("CREATE TABLE IF NOT EXISTS %s (b VARCHAR, a VARCHAR)", TEST_TABLE)
            };
            ConnectionPool.executeBatchInTxn(conn, setupQueries);

            // Detect database names
            List<String> dbs = new ArrayList<>();
            ConnectionPool.collectFirstColumn(conn, "SHOW DATABASES", String.class).forEach(dbs::add);
            // Detect metadata database
            detectedMetadataDatabase = dbs.stream().filter(d -> d != null && d.startsWith("__ducklake_metadata_")).findFirst().orElseThrow(() -> new IllegalStateException("Metadata DB not found. DBs: " + dbs));
            // Detect attached database
            detectedDatabase = dbs.stream().filter(d -> d != null && !"memory".equalsIgnoreCase(d) && !d.startsWith("__ducklake_metadata_")).findFirst().orElseThrow(() -> new IllegalStateException("Attached DB not found. DBs: " + dbs));
            // Get table ID from metadata
            String getTableIdSql = String.format("SELECT table_id FROM %s.ducklake_table WHERE table_name = '%s'", detectedMetadataDatabase, TEST_TABLE);
            tableId = ConnectionPool.collectFirst(conn, getTableIdSql, Long.class);
            assertNotNull(tableId, "Table ID should not be null");
        }
    }

    private static void startServer() throws Exception {
        // Start server with configuration
        String[] args = {
                "--conf", "dazzleduck_server.http.port=" + TEST_PORT,

                "--conf", "dazzleduck_server." + ConfigUtils.WAREHOUSE_CONFIG_KEY +
                "=\"" + warehousePath.toString().replace("\\", "/") + "\"",

                "--conf", "dazzleduck_server.ingestion.max_delay_ms=500",

                "--conf", "dazzleduck_server.post_ingestion_task_factory_provider.class=" +
                "io.dazzleduck.sql.commons.ingestion.DuckLakePostIngestionTaskFactoryProvider",

                "--conf", "dazzleduck_server.post_ingestion_task_factory_provider.catalog_name=\"" +
                DUCKLAKE_CATALOG + "\"",

                "--conf", "dazzleduck_server.post_ingestion_task_factory_provider.metadata_database=\"" +
                detectedMetadataDatabase + "\"",

                "--conf", "dazzleduck_server.post_ingestion_task_factory_provider.path_to_table_mapping.table_name=\"" +
                TEST_TABLE + "\"",

                "--conf", "dazzleduck_server.post_ingestion_task_factory_provider.path_to_table_mapping.catalog_name=\"" +
                DUCKLAKE_CATALOG + "\"",

                "--conf", "dazzleduck_server.post_ingestion_task_factory_provider.path_to_table_mapping.schema_name=main",

                "--conf", "dazzleduck_server.post_ingestion_task_factory_provider.path_to_table_mapping.base_path=\"" +
                INGEST_PATH + "\""
        };

        Main.main(args);
    }

    @AfterAll
    public static void cleanup() throws Exception {
        ConnectionPool.execute("DETACH " + DUCKLAKE_CATALOG);
    }

    @Test
    @Order(1)
    public void testDuckLakeSetup() throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            // Verify DuckLake catalog is attached
            String checkCatalogSql = "SELECT database_name FROM duckdb_databases() WHERE database_name = '" + DUCKLAKE_CATALOG + "'";
            var catalogs = ConnectionPool.collectFirstColumn(conn, checkCatalogSql, String.class).iterator();
            assertTrue(catalogs.hasNext(), "DuckLake catalog should be attached");
            String catalogName = catalogs.next();
            assertEquals(DUCKLAKE_CATALOG, catalogName, "Catalog name should match");
            assertFalse(catalogs.hasNext(), "Only one DuckLake catalog entry expected");
        }
    }

    @Test
    @Order(2)
    public void testIngestionWithDuckLakePostIngestion() throws Exception {
        String query = "SELECT * FROM ( VALUES ('apple',  'fruit'), ('banana', 'fruit'), ('carrot', 'vegetable')) AS t(b, a)";
        byte[] arrowData;
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection connection = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(connection, allocator, query, 1000);
             var outputStream = new ByteArrayOutputStream();
             var writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, outputStream)) {

            writer.start();
            while (reader.loadNextBatch()) {
                writer.writeBatch();
            }
            writer.end();
            arrowData = outputStream.toByteArray();
        }
        Files.createDirectories(warehousePath.resolve(INGEST_PATH));

        var request = HttpRequest.newBuilder(URI.create(BASE_URL + "/v1/ingest?path=" + INGEST_PATH))
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(arrowData)))
                .header("Content-Type", APPLICATION_ARROW).build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
    }

    @Test
    @Order(3)
    public void testFilesAddedToDuckLake() throws Exception {
        try (DuckDBConnection conn = ConnectionPool.getConnection();
             BufferAllocator allocator = new RootAllocator()) {
            ConnectionPool.execute(conn, "USE " + DUCKLAKE_CATALOG);
            // Row count
            Long count = ConnectionPool.collectFirst(conn, "SELECT COUNT(*) FROM " + TEST_TABLE, Long.class);
            assertEquals(3L, count, "DuckLake table should contain 3 rows");
            // EXPECTED RESULT SQL
            String expectedSql = "SELECT * FROM (VALUES ('apple',  'fruit'), ('banana', 'fruit'), ('carrot', 'vegetable')) AS t(b, a) ORDER BY b";
            // ACTUAL RESULT SQL
            String actualSql = "SELECT b, a FROM %s ORDER BY b".formatted(TEST_TABLE);
            TestUtils.isEqual(conn, allocator, expectedSql, actualSql);
        }
    }

    @Test
    @Order(4)
    public void testMultipleIngestions() throws Exception {
        // Ingest second batch (MUST RUN IN ORDER)
        String query = "SELECT * FROM ( VALUES ('apple',  'fruit'), ('banana', 'fruit'), ('carrot', 'vegetable')) AS t(b, a)";
        byte[] arrowData;
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection connection = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(connection, allocator, query, 1000);
             var byteArrayOutputStream = new ByteArrayOutputStream();
             var streamWriter = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, byteArrayOutputStream)) {

            streamWriter.start();
            while (reader.loadNextBatch()) {
                streamWriter.writeBatch();
            }
            streamWriter.end();
            arrowData = byteArrayOutputStream.toByteArray();
        }

        var request = HttpRequest.newBuilder(URI.create(BASE_URL + "/v1/ingest?path=" + INGEST_PATH))
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(arrowData)))
                .header("Content-Type", APPLICATION_ARROW).build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Second ingestion should succeed");
        // Verify total count
        String querySql = String.format("SELECT COUNT(*) FROM %s.%s", DUCKLAKE_CATALOG, TEST_TABLE);
        Long rowCount = ConnectionPool.collectFirst(querySql, Long.class);
        assertEquals(6L, rowCount, "Should have 6 rows after second ingestion");
    }
}