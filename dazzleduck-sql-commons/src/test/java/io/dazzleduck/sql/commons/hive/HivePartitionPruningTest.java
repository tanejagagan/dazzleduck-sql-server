package io.dazzleduck.sql.commons.hive;


import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.FileStatus;
import io.dazzleduck.sql.commons.S3MockContainerTestUtil;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class HivePartitionPruningTest {

    static final String basePath = "example/data/hive_table";
    static final String[][] partition = {{"dt", "date"}, {"p", "string"}};
    public static final String CREATE_SECRET_SQL =  "CREATE SECRET %s ( %s )";
    public static final String INSERT_STATEMENT = "COPY " +
            "    (FROM generate_series(10)) " +
            "    TO '%s' " +
            "    (FORMAT parquet)";


    public static Network network = Network.newNetwork();
    public static GenericContainer<?> s3mock =
            S3MockContainerTestUtil.createContainer("s3mock", network);


    @BeforeAll
    public static void setup() throws IOException, SQLException {
        s3mock.start();
        createDuckDBSecret();
        insertDataUsingDuckDB();
    }

    static String quote(String input) {
        return String.format("'%s'", input);
    }
    private static void createDuckDBSecret() {
        var secret = S3MockContainerTestUtil.duckDBSecretForS3Access(s3mock);
        String param = "TYPE s3" +
        ",KEY_ID " +  quote(secret.get("KEY_ID")) +
                ",SECRET " +  quote(secret.get("SECRET")) +
                ",ENDPOINT " +  quote(secret.get("ENDPOINT")) +
                ",USE_SSL " +  "false" +
                ",URL_STYLE " +  "path";
       ConnectionPool.execute(String.format(CREATE_SECRET_SQL, "d", param));
    }

    private static void  insertDataUsingDuckDB() throws SQLException, IOException {
        String path = "s3://" + S3MockContainerTestUtil.TEST_BUCKET_NAME + "/hive_table/dt=2024-01-01/p=x/result.parquet";
        ConnectionPool.execute(String.format(INSERT_STATEMENT, path));
    }


    @Test
    public void getQueryString() throws SQLException, IOException {
        String queryString = HivePartitionPruning.getQueryString(basePath, 2);
        String countSql = String.format("with t as (%s) " +
                "select count(*) from t", queryString);
        assertEquals(3, ConnectionPool.collectFirst(countSql, Long.class));
    }

    @Test
    public void testPruneFile() throws SQLException, IOException {
        for(int i =0; i < 10; i ++) {
            assertSize(3, basePath, "true", partition);
        }
        assertSize(1, basePath,"p = 'a b'", partition);
        assertSize(2, basePath, "dt = '2025-01-01'", partition);
        for(int i = 0 ; i < 10; i ++) {
            assertSize(0, basePath, "dt = '2023-01-01'", partition);
        }
    }

    private static void assertSize(int expectedSize, String basePath, String filter, String[][] partition) throws SQLException, IOException {
        List<FileStatus> result = HivePartitionPruning.pruneFiles(basePath, filter, partition);
        assertEquals(expectedSize, result.size(), result.stream().map(Record::toString).collect(Collectors.joining(",")));
    }

    @Test
    public void testPruneFileNoPartition() throws SQLException, IOException {
        assertSize(1, basePath + "/dt=2024-01-01/p=x", "true", new String[0][0]);
    }

    @Test
    public void testPruneFileS3() throws SQLException, IOException {
        String path = "s3://" + S3MockContainerTestUtil.TEST_BUCKET_NAME + "/hive_table";
        assertSize(1, path, "true", partition);
    }

    @Test
    public void testPruneFileS3NoPartition() throws SQLException, IOException {
        String path = "s3://" + S3MockContainerTestUtil.TEST_BUCKET_NAME + "/hive_table/dt=2024-01-01/p=x";
        assertSize(1, path, "true", new String[0][0]);
    }

    @Test
    public void testCast() throws SQLException, IOException {
       String filter = "CAST(\"dt\" as DATE) IS NOT NULL AND CAST(\"dt\" as DATE) = '2025-01-01'";
       assertSize(2, basePath, filter, partition);
    }

    @Test
    public void testReader() throws SQLException, IOException {
        for (int i = 0; i < 1000; i++) {
            try(BufferAllocator allocator = new RootAllocator();
                DuckDBConnection connection = ConnectionPool.getConnection();
                ArrowReader reader = ConnectionPool.getReader(connection, allocator, "select * from (select 1 as one)", 1000)) {
                while (reader.loadNextBatch()) {
                    reader.getVectorSchemaRoot();
                }
            }
        }
    }

    @Test
    public void testWriter() {

    }

    /** Files as a set, sorted by name: pruneFiles orders only by lastModified, which can tie. */
    private static List<FileStatus> byName(List<FileStatus> files) {
        return files.stream().sorted(java.util.Comparator.comparing(FileStatus::fileName)).toList();
    }

    /**
     * Runs {@code body} with DuckDB's arrow_large_buffer_size on, then restores the value it had.
     * The setting is GLOBAL, so it applies to the shared pool's every connection: this relies on the
     * module's tests running one at a time (surefire's default; no JUnit parallel execution here).
     */
    static void withLargeArrowBuffers(org.junit.jupiter.api.function.Executable body) throws Throwable {
        String previous;
        try (DuckDBConnection connection = ConnectionPool.getConnection();
             var st = connection.createStatement();
             var rs = st.executeQuery("SELECT current_setting('arrow_large_buffer_size')::VARCHAR")) {
            rs.next();
            previous = rs.getString(1);
        }
        ConnectionPool.execute("SET GLOBAL arrow_large_buffer_size = true");
        try {
            body.execute();
        } finally {
            ConnectionPool.execute("SET GLOBAL arrow_large_buffer_size = " + previous);
        }
    }

    /**
     * DuckDB's arrow_large_buffer_size sends LargeList and LargeUtf8 instead of List and Utf8.
     * Pruning must return the same files either way.
     */
    @Test
    public void pruningIsTheSameWithLargeArrowBuffers() throws Throwable {
        String[] filters = {"true", "p = 'a b'", "dt = '2025-01-01'", "dt = '2023-01-01'",
                "CAST(\"dt\" as DATE) IS NOT NULL AND CAST(\"dt\" as DATE) = '2025-01-01'"};
        String unpartitioned = basePath + "/dt=2024-01-01/p=x";
        List<List<FileStatus>> expected = new java.util.ArrayList<>();
        for (String filter : filters) {
            expected.add(byName(HivePartitionPruning.pruneFiles(basePath, filter, partition)));
        }
        List<FileStatus> expectedUnpartitioned = byName(HivePartitionPruning.pruneFiles(unpartitioned, "true", new String[0][0]));
        assertFalse(expected.get(0).isEmpty(), "sanity: there are files to prune");

        withLargeArrowBuffers(() -> {
            try (DuckDBConnection connection = ConnectionPool.getConnection();
                 BufferAllocator allocator = new RootAllocator();
                 ArrowReader reader = ConnectionPool.getReader(connection, allocator, "SELECT 'x' AS s", 10)) {
                assertEquals("LargeUtf8", reader.getVectorSchemaRoot().getSchema().findField("s").getType().getTypeID().name(),
                        "the setting is in effect");
            }
            for (int f = 0; f < filters.length; f++) {
                assertEquals(expected.get(f), byName(HivePartitionPruning.pruneFiles(basePath, filters[f], partition)), filters[f]);
            }
            assertEquals(expectedUnpartitioned, byName(HivePartitionPruning.pruneFiles(unpartitioned, "true", new String[0][0])));
        });
    }

    /** UNESCAPE_FN on each list layout DuckDB sends, from a database of its own (the setting is GLOBAL). */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void unescapeReadsEitherListLayout(boolean largeBuffers) throws Exception {
        String sql = "SELECT * FROM (VALUES (1, ['a%20b', NULL, 'x']), (2, NULL), (3, [])) t(i, partitions) ORDER BY i";
        try (DuckDBConnection connection = (DuckDBConnection) java.sql.DriverManager.getConnection("jdbc:duckdb:");
             BufferAllocator allocator = new RootAllocator()) {
            try (var st = connection.createStatement()) {
                st.execute("SET arrow_large_buffer_size = " + largeBuffers);
            }
            try (ArrowReader reader = ConnectionPool.getReader(connection, allocator, sql, 10);
                 var target = (org.apache.arrow.vector.complex.ListVector)
                         HivePartitionPruning.UNSCAPE_PARTITION_FIELD.createVector(allocator)) {
                assertEquals(true, reader.loadNextBatch());
                var partitions = reader.getVectorSchemaRoot().getVector("partitions");
                assertEquals(largeBuffers ? "LargeList" : "List", partitions.getField().getType().getTypeID().name());
                target.allocateNew();
                target.setValueCount(partitions.getValueCount());
                HivePartitionPruning.UNESCAPE_FN.apply(List.of(partitions), target);
                assertEquals("[\"a b\",null,\"x\"]", String.valueOf(target.getObject(0)), "escaped value, null value");
                assertEquals("[]", String.valueOf(target.getObject(1)), "a null list stays empty");
                assertEquals("[]", String.valueOf(target.getObject(2)));
            }
        }
    }
}
