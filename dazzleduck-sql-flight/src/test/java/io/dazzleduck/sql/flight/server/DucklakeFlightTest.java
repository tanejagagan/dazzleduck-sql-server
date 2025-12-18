package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTaskFactoryProvider;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.flight.FlightRecorder;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.optimizer.QueryOptimizer;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

import static io.dazzleduck.sql.commons.util.TestConstants.SUPPORTED_HIVE_PATH_QUERY;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class DucklakeFlightTest {
    private static final String LOCALHOST = "localhost";
    private static final String USER = "admin";
    private static final String PASSWORD = "password";
    private static final String CATALOG = "test_ducklake";
    private static final String SCHEMA = "main";
    private static final String PARTITIONED_TABLE = "tt_p";
    private static final String NON_PARTITIONED_TABLE = "tt";
    private static final String METADATA_DATABASE = "__ducklake_metadata_" + CATALOG;

    private static final BufferAllocator clientAllocator = new RootAllocator(Integer.MAX_VALUE);
    private static final BufferAllocator serverAllocator = new RootAllocator(Integer.MAX_VALUE);

    private static FlightServer flightServer;
    private static FlightSqlClient sqlClient;
    private static DuckDBFlightSqlProducer producer;
    private static String warehousePath;
    private static String workspace;
    private static ServerClient serverClient;

    @BeforeAll
    public static void setup() throws Exception {
        // Create workspace and warehouse directories
        workspace = Files.createTempDirectory("ducklake_test_workspace_").toString();
        warehousePath = Files.createTempDirectory("ducklake_warehouse_").toString();

        // Setup ducklake database
        setupDucklakeDatabase();

        // Setup Flight server and client
        setupFlightServerAndClient();
    }

    private static void setupDucklakeDatabase() {
        String[] setups = {
                "INSTALL ducklake",
                "LOAD  ducklake",
                "ATTACH 'ducklake:%s/metadata' AS %s (DATA_PATH '%s/data')".formatted(workspace, CATALOG, workspace),
        };
        ConnectionPool.executeBatch(setups);

        // Create test tables
        createTestTable(CATALOG + "." + SCHEMA + "." + PARTITIONED_TABLE, true);
        addDataFile(PARTITIONED_TABLE);

        createTestTable(CATALOG + "." + SCHEMA + "." + NON_PARTITIONED_TABLE, false);
        addDataFile(NON_PARTITIONED_TABLE);
    }

    private static void createTestTable(String table, boolean partitioned) {
        String[] statements = getStatements(table, partitioned);
        for (var statement : statements) {
            ConnectionPool.execute(statement);
        }
    }

    private static String[] getStatements(String table, boolean partition) {
        var createStatement = "CREATE TABLE %s(key string, value string, partition int)".formatted(table);
        var insertStatement0 = "INSERT INTO %s VALUES ('k00', 'v00', 0), ('k01', 'v01', 0)".formatted(table);
        var partitionTable = partition ? "ALTER TABLE %s SET PARTITIONED BY (partition)".formatted(table)
                : "select 1";
        var insertStatement1 = "INSERT INTO %s VALUES (null, null, 1)".formatted(table);
        var insertStatement2 = "INSERT INTO %s VALUES ('k11', 'v11', 1), ('k21', 'v21', 1)".formatted(table);
        var insertStatement3 = "INSERT INTO %s VALUES ('k31', 'v31', 1), ('k41', 'v41', 1)".formatted(table);
        var insertStatement4 = "INSERT INTO %s VALUES ('k51', 'v51', 1), ('k61', 'v61', 1)".formatted(table);
        var addColumn = "ALTER TABLE %s ADD COLUMN version INT".formatted(table);
        var insertStatement5 = "INSERT INTO %s VALUES ('k51', 'v51', 1, 0), ('k61', 'v61', 1, 0)".formatted(table);
        var insertStatement6 = "INSERT INTO %s VALUES ('k72', 'v72', 2, 0), ('k82', 'v82', 2, 0)".formatted(table);
        return new String[]{
                createStatement,
                insertStatement0,
                partitionTable,
                insertStatement1,
                insertStatement2,
                insertStatement3,
                insertStatement4,
                addColumn,
                insertStatement5,
                insertStatement6,
        };
    }

    private static void addDataFile(String table) {
        String path = workspace + "/data/main/" + table + "/manual.parquet";
        String sql = "COPY (SELECT * FROM (VALUES('k119', 'v119', 9, 0),('k118', 'v118', 8, 0)) AS my_value(key, value, partition, version)) TO '%s' (FORMAT parquet)".formatted(path);
        String addTableSql = "CALL ducklake_add_data_files('%s', '%s', '%s', allow_missing => true)".formatted(CATALOG, table, path);
        String[] batch = {sql, addTableSql};
        ConnectionPool.executeBatch(batch);
    }

    private static void setupFlightServerAndClient() throws Exception {
        final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 55580);
        String producerId = UUID.randomUUID().toString();
        FlightRecorder recorder = new MicroMeterFlightRecorder(new SimpleMeterRegistry(), producerId);

        producer = new DuckDBFlightSqlProducer(
                serverLocation,
                producerId,
                "change me",
                serverAllocator,
                warehousePath,
                AccessMode.RESTRICTED,
                DuckDBFlightSqlProducer.newTempDir(),
                PostIngestionTaskFactoryProvider.NO_OP.getPostIngestionTaskFactory(),
                Executors.newSingleThreadScheduledExecutor(),
                Duration.ofMinutes(2),
                Clock.systemDefaultZone(),
                recorder,
                QueryOptimizer.NOOP_QUERY_OPTIMIZER);


        FlightTestUtils utils  = FlightTestUtils.createForDatabaseSchema(USER, "admin", CATALOG, SCHEMA);
        Location location = FlightTestUtils.findNextLocation();
        serverClient = utils.createRestrictedServerClient(location, Map.of());
        flightServer = serverClient.flightServer();
        sqlClient = serverClient.flightSqlClient();
    }
    @AfterAll
    public static void cleanup() throws Exception {
        serverClient.close();
        ConnectionPool.execute("DETACH %s".formatted( CATALOG));
    }

    @Test
    public void testSimpleReadDucklakeQuery() throws Exception {

        final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 55559);
        try ( var serverClient = createRestrictedServerClient( serverLocation, "admin" )) {
            try (var splittableClient = splittableAdminClientForTable(serverLocation, serverClient.clientAllocator(), PARTITIONED_TABLE)) {

                var flightCallHeaders = new FlightCallHeaders();
                flightCallHeaders.insert(Headers.HEADER_SPLIT_SIZE, "1");
                var flightInfo = splittableClient.execute("select * from %s where key = 'k51'".formatted(PARTITIONED_TABLE),
                        new HeaderCallOption(flightCallHeaders));
                assertEquals(2, flightInfo.getEndpoints().size());
                var size = 0;
                for (var endpoint : flightInfo.getEndpoints()) {
                    try (final FlightStream stream = splittableClient.getStream(endpoint.getTicket(), new HeaderCallOption(flightCallHeaders))) {
                        while (stream.next()) {
                            size+=stream.getRoot().getRowCount();
                        }
                    }
                }
                assertEquals(2, size);
            }
        }
    }

    private FlightSqlClient splittableAdminClientForTable( Location location, BufferAllocator allocator, String table) {
        return new FlightSqlClient(FlightClient.builder(allocator, location)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER,
                        PASSWORD,
                        Map.of(Headers.HEADER_DATABASE, CATALOG,
                                Headers.HEADER_SCHEMA, SCHEMA,
                                Headers.HEADER_TABLE, table)))
                .build());
    }
    private ServerClient createRestrictedServerClient(Location serverLocation,
                                                      String user) throws IOException, NoSuchAlgorithmException {
        var testUtil = FlightTestUtils.createForDatabaseSchema(user, "",  CATALOG, SCHEMA);
        return testUtil.createRestrictedServerClient(serverLocation, Map.of());
    }

    @Test
    public void testReadDucklakeWithFilter() throws Exception {
        String query = "SELECT * FROM read_ducklake('%s.%s.%s') WHERE key = 'k51'".formatted(CATALOG, SCHEMA, PARTITIONED_TABLE);
        final FlightInfo flightInfo = sqlClient.execute(query);

        int rowCount = 0;
        try (final FlightStream stream = sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
            while (stream.next()) {
                rowCount += stream.getRoot().getRowCount();
            }
        }

        // Should find the rows with key = 'k51'
        assertTrue(rowCount > 0, "Should have found rows matching filter");
    }

    @Test
    public void testDucklakeSplittableQuery() throws Exception {
        String query = "SELECT * FROM read_ducklake('%s.%s.%s')".formatted(CATALOG, SCHEMA, PARTITIONED_TABLE);

        var flightCallHeaders = new FlightCallHeaders();
        flightCallHeaders.insert(Headers.HEADER_SPLIT_SIZE, "1");
        var callOption = new HeaderCallOption(flightCallHeaders);

        final FlightInfo flightInfo = sqlClient.execute(query, callOption);

        // Should have multiple endpoints due to splitting
        assertTrue(flightInfo.getEndpoints().size() > 0, "Should have at least one endpoint");

        int totalRows = 0;
        for (var endpoint : flightInfo.getEndpoints()) {
            try (final FlightStream stream = sqlClient.getStream(endpoint.getTicket(), callOption)) {
                while (stream.next()) {
                    totalRows += stream.getRoot().getRowCount();
                }
            }
        }

        assertTrue(totalRows > 0, "Should have read data from all endpoints");
    }

    @Test
    public void testDucklakeMetadataExists() throws Exception {
        // Verify that the metadata database was created properly
        String query = "SELECT count(*) as cnt FROM %s.ducklake_table".formatted(METADATA_DATABASE);
        ConnectionPool.printResult(query);

        Long count = ConnectionPool.collectFirst(query, Long.class);
        assertNotNull(count);
        assertTrue(count >= 2, "Should have at least 2 tables created (partitioned and non-partitioned)");
    }
}
