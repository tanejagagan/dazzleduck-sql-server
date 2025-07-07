package io.dazzleduck.sql.commons.hive;


import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.FileStatus;
import io.dazzleduck.sql.commons.MinioContainerTestUtil;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.errors.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.net.URI;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HivePartitionPruningTest {

    static final String basePath = "example/hive_table";
    static final String[][] partition = {{"dt", "date"}, {"p", "string"}};
    public static final String CREATE_SECRET_SQL =  "CREATE SECRET %s ( %s )";
    public static final String INSERT_STATEMENT = "COPY " +
            "    (FROM generate_series(10)) " +
            "    TO '%s' " +
            "    (FORMAT parquet)";


    public static Network network = Network.newNetwork();
    public static MinIOContainer minio =
            MinioContainerTestUtil.createContainer("minio", network);
    public static MinioClient minioClient;


    @BeforeAll
    public static void setup() throws IOException, ServerException, InsufficientDataException, ErrorResponseException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException, SQLException {
        minio.start();
        minioClient = MinioContainerTestUtil.createClient(minio);
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(MinioContainerTestUtil.TEST_BUCKET_NAME).build());
        createDuckDBSecret();
        insertDataUsingDuckDB();
    }

    static String quote(String input) {
        return String.format("'%s'", input);
    }
    private static void createDuckDBSecret() {
        var uri = URI.create(minio.getS3URL());
        String param = "TYPE s3" +
        ",KEY_ID " +  quote( minio.getUserName()) +
                ",SECRET " +  quote(minio.getPassword()) +
                ",ENDPOINT " +  quote (uri.getHost() + ":" + uri.getPort()) +
                ",USE_SSL " +  "false" +
                ",URL_STYLE " +  "path";
       ConnectionPool.execute(String.format(CREATE_SECRET_SQL, "d", param));
    }

    private static void  insertDataUsingDuckDB() throws SQLException, IOException {
        String path = "s3://" + MinioContainerTestUtil.TEST_BUCKET_NAME + "/hive_table/dt=2024-01-01/p=x/result.parquet";
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
        String path = "s3://" + MinioContainerTestUtil.TEST_BUCKET_NAME + "/hive_table";
        assertSize(1, path, "true", partition);
    }

    @Test
    public void testPruneFileS3NoPartition() throws SQLException, IOException {
        String path = "s3://" + MinioContainerTestUtil.TEST_BUCKET_NAME + "/hive_table/dt=2024-01-01/p=x";
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
}


