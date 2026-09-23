package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DuckLakeIngestionHandlerTest {

    @TempDir
    Path tempDir;

    static final String CATALOG = "test_factory_lake";
    static final String SCHEMA = "main";
    static final String TABLE = "events";
    static final String QUEUE_ID = "events";

    @BeforeEach
    void setUp() throws Exception {
        Files.createDirectories(tempDir.resolve("data"));
        ConnectionPool.execute("INSTALL arrow FROM community");
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "LOAD arrow",
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(
                            tempDir.resolve("catalog"), CATALOG, tempDir.resolve("data")),
                    "CREATE TABLE %s.%s.%s (id BIGINT, msg VARCHAR)".formatted(CATALOG, SCHEMA, TABLE)
            });
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        }
    }

    private QueueIdToTableMapping mapping(String queueId, String transformation) {
        return new QueueIdToTableMapping(queueId, CATALOG, SCHEMA, TABLE, Map.of(), transformation);
    }

    // -----------------------------------------------------------------------
    // getTargetPath
    // -----------------------------------------------------------------------

    @Test
    void shouldResolveTargetPathFromCatalogOnConstruction() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        String path = factory.getTargetPath(QUEUE_ID);
        assertNotNull(path, "Expected resolved path, got null");
        assertFalse(path.isBlank());
        assertTrue(path.contains(tempDir.resolve("data").toString()),
                "Expected path under data dir, got: " + path);
    }

    @Test
    void shouldReturnNullTargetPathForUnknownQueueId() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        assertNull(factory.getTargetPath("unknown-queue"));
    }

    /**
     * Regression: a queue added via {@link DuckLakeIngestionHandler#updateMappings} (as the dynamic
     * handler does) has no cached state yet — its path must still be derived lazily on first access.
     * This was broken when the state-key lookup consulted the state cache instead of the mappings.
     */
    @Test
    void shouldResolveTargetPathForQueueAddedViaUpdateMappings() {
        var factory = new DuckLakeIngestionHandler(Map.of()); // empty: no eagerly-built state
        assertNull(factory.getTargetPath(QUEUE_ID), "unknown before it is registered");

        factory.updateMappings(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));

        String path = factory.getTargetPath(QUEUE_ID);
        assertNotNull(path, "path must be derived lazily for a queue added via updateMappings");
        assertTrue(path.contains(tempDir.resolve("data").toString()), "got: " + path);
    }

    // -----------------------------------------------------------------------
    // createPostIngestionTask — direct match
    // -----------------------------------------------------------------------

    @Test
    void shouldCreateTaskForDirectQueueIdMatch() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        var result = new IngestionResult(QUEUE_ID, 1L, "app", Map.of(), 0L, List.of());
        PostIngestionTask task = factory.createPostIngestionTask(result);
        assertNotNull(task);
        assertInstanceOf(DuckLakePostIngestionTask.class, task);
    }

    // -----------------------------------------------------------------------
    // createPostIngestionTask — suffix fallback
    // -----------------------------------------------------------------------

    @Test
    void shouldCreateTaskUsingSuffixFallbackWhenQueueNameIsAPath() {
        // Mapping key is "events"; result queueName is a path whose last segment is "events"
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        String pathQueueName = "/tmp/otel-output/" + QUEUE_ID;
        var result = new IngestionResult(pathQueueName, 1L, "app", Map.of(), 0L, List.of());
        PostIngestionTask task = factory.createPostIngestionTask(result);
        assertNotNull(task);
        assertInstanceOf(DuckLakePostIngestionTask.class, task);
    }

    @Test
    void shouldCreateTaskUsingSuffixFallbackWithBackslashPath() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        String windowsPath = "C:\\output\\" + QUEUE_ID;
        var result = new IngestionResult(windowsPath, 1L, "app", Map.of(), 0L, List.of());
        PostIngestionTask task = factory.createPostIngestionTask(result);
        assertNotNull(task);
        assertInstanceOf(DuckLakePostIngestionTask.class, task);
    }

    // -----------------------------------------------------------------------
    // createPostIngestionTask — no mapping found → NOOP (not an error)
    // -----------------------------------------------------------------------

    @Test
    void shouldReturnNoopForUnknownQueueIdWithNoSuffixMatch() {
        // A unified handler may serve signals that have no DuckLake mapping (e.g. traces/metrics
        // in a config where only logs are registered in DuckLake). NOOP is the correct result.
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        var result = new IngestionResult("completely-unknown", 1L, "app", Map.of(), 0L, List.of());
        PostIngestionTask task = factory.createPostIngestionTask(result);
        assertNotNull(task, "Should return NOOP task, not null");
        task.execute(); // must not throw
    }

    @Test
    void shouldReturnNoopWhenPathSuffixDoesNotMatchAnyMappingKey() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        // last segment is "other", not "events"
        var result = new IngestionResult("/tmp/output/other", 1L, "app", Map.of(), 0L, List.of());
        PostIngestionTask task = factory.createPostIngestionTask(result);
        assertNotNull(task, "Should return NOOP task, not null");
        task.execute(); // must not throw
    }

    // -----------------------------------------------------------------------
    // getTransformation
    // -----------------------------------------------------------------------

    @Test
    void shouldReturnTransformationForKnownQueue() {
        String sql = "SELECT id, upper(msg) AS msg FROM __this";
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, sql)));
        assertEquals(sql, factory.getTransformation(QUEUE_ID));
    }

    @Test
    void shouldReturnNullTransformationWhenNotConfigured() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        assertNull(factory.getTransformation(QUEUE_ID));
    }

    @Test
    void shouldReturnNullTransformationForUnknownQueue() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        assertNull(factory.getTransformation("no-such-queue"));
    }

    @Test
    void shouldReturnTransformationViaSuffixFallback() {
        String sql = "SELECT * FROM __this WHERE id > 0";
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, sql)));
        assertEquals(sql, factory.getTransformation("/var/data/" + QUEUE_ID));
    }

    // -----------------------------------------------------------------------
    // Multiple mappings
    // -----------------------------------------------------------------------

    @Test
    void shouldSupportMultipleMappings() throws Exception {
        String table2 = "metrics";
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "CREATE TABLE %s.%s.%s (id BIGINT, val DOUBLE)".formatted(CATALOG, SCHEMA, table2));
        }

        var mappings = Map.of(
                "events", new QueueIdToTableMapping("events", CATALOG, SCHEMA, TABLE, Map.of(), null),
                "metrics", new QueueIdToTableMapping("metrics", CATALOG, SCHEMA, table2, Map.of(), null)
        );
        var factory = new DuckLakeIngestionHandler(mappings);

        assertNotNull(factory.getTargetPath("events"));
        assertNotNull(factory.getTargetPath("metrics"));
        assertNotEquals(factory.getTargetPath("events"), factory.getTargetPath("metrics"));

        assertInstanceOf(DuckLakePostIngestionTask.class,
                factory.createPostIngestionTask(new IngestionResult("events", 1L, "app", Map.of(), 0L, List.of())));
        assertInstanceOf(DuckLakePostIngestionTask.class,
                factory.createPostIngestionTask(new IngestionResult("metrics", 1L, "app", Map.of(), 0L, List.of())));
    }

    // -----------------------------------------------------------------------
    // getPartitionBy() - partitioned table
    // -----------------------------------------------------------------------

    @Test
    void shouldReturnPartitionColumnsForPartitionedTable() throws Exception {
        String partitionedTable = "partitioned_events";
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "CREATE TABLE %s.%s.%s (id BIGINT, value VARCHAR, date DATE, level VARCHAR)".formatted(CATALOG, SCHEMA, partitionedTable),
                    "ALTER TABLE %s.%s.%s SET PARTITIONED BY (date, level)".formatted(CATALOG, SCHEMA, partitionedTable)
            });
        }

        var mapping = new QueueIdToTableMapping(partitionedTable, CATALOG, SCHEMA, partitionedTable, Map.of(), null);
        var factory = new DuckLakeIngestionHandler(Map.of(partitionedTable, mapping));
        String[] partitionColumns = factory.getPartitionBy(partitionedTable);
        assertNotNull(partitionColumns, "Expected non-null partition columns array");
        assertEquals(2, partitionColumns.length, "Expected exactly 2 partition columns");
        assertEquals("date", partitionColumns[0], "Expected first partition column to be 'date'");
        assertEquals("level", partitionColumns[1], "Expected second partition column to be 'level'");
    }

    // -----------------------------------------------------------------------
    // getPartitionBy() - non-partitioned table
    // -----------------------------------------------------------------------

    @Test
    void shouldReturnEmptyArrayForNonPartitionedTable() throws Exception {
        String nonPartitionedTable = "non_partitioned_events";
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn,
                    "CREATE TABLE %s.%s.%s (id BIGINT, value VARCHAR)".formatted(CATALOG, SCHEMA, nonPartitionedTable));
        }

        var mapping = new QueueIdToTableMapping(nonPartitionedTable, CATALOG, SCHEMA, nonPartitionedTable, Map.of(), null);
        var factory = new DuckLakeIngestionHandler(Map.of(nonPartitionedTable, mapping));

        String[] partitionColumns = factory.getPartitionBy(nonPartitionedTable);
        assertNotNull(partitionColumns, "Expected non-null partition columns array");
        assertEquals(0, partitionColumns.length, "Expected empty array for non-partitioned table");
    }

    // -----------------------------------------------------------------------
    // Time-transform partition columns (year/month/day/hour) — PR #363
    //
    // DuckLake stores a partition column's `transform` as the transform name for time transforms
    // (year/month/day/hour) and "identity" for plain columns. A time transform cannot be named
    // directly in COPY's PARTITION_BY (which accepts only column names), so the handler resolves it
    // into a token (the transform name) plus a derived-column projection that must be added to the
    // COPY relation for the token to resolve.
    // -----------------------------------------------------------------------

    private DuckLakeIngestionHandler handlerForPartitionedTable(String table, String partitionBySpec,
                                                                String columnsDdl) throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "CREATE TABLE %s.%s.%s (%s)".formatted(CATALOG, SCHEMA, table, columnsDdl),
                    "ALTER TABLE %s.%s.%s SET PARTITIONED BY (%s)".formatted(CATALOG, SCHEMA, table, partitionBySpec)
            });
        }
        var mapping = new QueueIdToTableMapping(table, CATALOG, SCHEMA, table, Map.of(), null);
        return new DuckLakeIngestionHandler(Map.of(table, mapping));
    }

    @Test
    void shouldReturnTransformNamesAsPartitionTokensForTimeTransforms() throws Exception {
        String table = "ts_transform_events";
        var factory = handlerForPartitionedTable(table,
                "year(ts), month(ts), day(ts), hour(ts)", "id BIGINT, ts TIMESTAMP");

        // PARTITION_BY receives the transform name (lower-cased), not the source column.
        assertArrayEquals(new String[]{"year", "month", "day", "hour"},
                factory.getPartitionBy(table),
                "Time transforms must be partitioned by the transform name, not the source column");
    }

    @Test
    void shouldReturnDerivedProjectionsForTimeTransforms() throws Exception {
        String table = "ts_projection_events";
        var factory = handlerForPartitionedTable(table,
                "year(ts), month(ts), day(ts), hour(ts)", "id BIGINT, ts TIMESTAMP");

        // Each transform is projected as `fn("col") AS "fn"` so the PARTITION_BY token resolves.
        assertArrayEquals(new String[]{
                        "year(\"ts\") AS \"year\"",
                        "month(\"ts\") AS \"month\"",
                        "day(\"ts\") AS \"day\"",
                        "hour(\"ts\") AS \"hour\""},
                factory.getPartitionProjections(table),
                "Expected one derived-column projection per time transform");
    }

    @Test
    void shouldReturnNoProjectionsForIdentityPartitionedTable() throws Exception {
        String table = "identity_partitioned_events";
        var factory = handlerForPartitionedTable(table,
                "date, level", "id BIGINT, date DATE, level VARCHAR");

        // Identity columns already exist in the relation — they are partitioned directly and need
        // no projection.
        assertArrayEquals(new String[]{"date", "level"}, factory.getPartitionBy(table));
        assertEquals(0, factory.getPartitionProjections(table).length,
                "Identity partitions must not emit derived-column projections");
    }

    @Test
    void shouldResolveMixedIdentityAndTransformPartitions() throws Exception {
        String table = "mixed_partitioned_events";
        var factory = handlerForPartitionedTable(table,
                "region, day(ts)", "id BIGINT, region VARCHAR, ts TIMESTAMP");

        // Order follows partition_key_index: identity first, then the transform.
        assertArrayEquals(new String[]{"region", "day"}, factory.getPartitionBy(table));
        // Only the transform contributes a projection; the identity column does not.
        assertArrayEquals(new String[]{"day(\"ts\") AS \"day\""},
                factory.getPartitionProjections(table));
    }

    @Test
    void shouldReturnEmptyProjectionsForNonPartitionedTable() throws Exception {
        String table = "no_partition_projections";
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn,
                    "CREATE TABLE %s.%s.%s (id BIGINT, value VARCHAR)".formatted(CATALOG, SCHEMA, table));
        }
        var mapping = new QueueIdToTableMapping(table, CATALOG, SCHEMA, table, Map.of(), null);
        var factory = new DuckLakeIngestionHandler(Map.of(table, mapping));

        assertEquals(0, factory.getPartitionProjections(table).length);
    }

    @Test
    void shouldReturnEmptyProjectionsForUnknownQueue() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        String[] projections = factory.getPartitionProjections("no-such-queue");
        assertNotNull(projections, "Unknown queue must yield an empty array, not null");
        assertEquals(0, projections.length);
    }

    /**
     * End-to-end invariant that {@code ParquetIngestionQueue} relies on: adding the handler's
     * projections to the relation makes every PARTITION_BY token a resolvable column. Builds the
     * same wrapped relation the queue builds ({@code SELECT *, <projections> FROM (...)}) and asserts
     * each token resolves to a real column.
     */
    @Test
    void derivedProjectionsMakePartitionTokensResolvable() throws Exception {
        String table = "resolvable_tokens_events";
        var factory = handlerForPartitionedTable(table,
                "year(ts), month(ts), day(ts), hour(ts)", "id BIGINT, ts TIMESTAMP");

        String[] tokens = factory.getPartitionBy(table);
        String[] projections = factory.getPartitionProjections(table);

        String wrapped = "SELECT *, %s FROM (SELECT * FROM %s.%s.%s)".formatted(
                String.join(", ", projections), CATALOG, SCHEMA, table);

        // If a token did not resolve to a column in the wrapped relation, selecting it would throw.
        String tokenList = String.join(", ",
                java.util.Arrays.stream(tokens).map(t -> "\"" + t + "\"").toArray(String[]::new));
        try (Connection conn = ConnectionPool.getConnection()) {
            assertDoesNotThrow(() ->
                    ConnectionPool.execute(conn, "SELECT %s FROM (%s) LIMIT 0".formatted(tokenList, wrapped)),
                    "Every PARTITION_BY token must resolve as a column in the projected relation");
        }
    }

    // -----------------------------------------------------------------------
    // bucket(N) hash-partition transform
    //
    // COPY's PARTITION_BY needs a real column, and DuckLake's bucket() is murmur3 with no DuckDB
    // builtin, so the handler projects _dd_iceberg_bucket(col, N). The hive key follows DuckLake's
    // own directory naming ("bucket", then "bucket_<col>" on collision) so ducklake_add_data_files
    // — which matches files to partitioning by hive key name — accepts the written files.
    // -----------------------------------------------------------------------

    @Test
    void shouldResolveBucketTransformsWithDuckLakeHiveKeys() throws Exception {
        String table = "bucketed_events";
        var factory = handlerForPartitionedTable(table,
                "bucket(4, group_id), event_date, bucket(4, user_id)",
                "group_id BIGINT, user_id BIGINT, event_date DATE");

        // First bucket dir is "bucket"; the second collides and is disambiguated as "bucket_user_id"
        // (DuckLake's own writer convention). The identity column keeps its name.
        assertArrayEquals(new String[]{"bucket", "event_date", "bucket_user_id"},
                factory.getPartitionBy(table));
        // Identity contributes no projection; each bucket column projects the murmur3 bucket macro.
        assertArrayEquals(new String[]{
                        "_dd_iceberg_bucket(\"group_id\", 4) AS \"bucket\"",
                        "_dd_iceberg_bucket(\"user_id\", 4) AS \"bucket_user_id\""},
                factory.getPartitionProjections(table));
    }

    @Test
    void bucketProjectionsMakePartitionTokensResolvable() throws Exception {
        String table = "bucket_resolvable_events";
        var factory = handlerForPartitionedTable(table,
                "bucket(8, group_id), event_date, bucket(8, user_id)",
                "group_id BIGINT, user_id BIGINT, event_date DATE");

        String[] tokens = factory.getPartitionBy(table);
        String[] projections = factory.getPartitionProjections(table);
        String wrapped = "SELECT *, %s FROM (SELECT * FROM %s.%s.%s)".formatted(
                String.join(", ", projections), CATALOG, SCHEMA, table);
        String tokenList = String.join(", ",
                java.util.Arrays.stream(tokens).map(t -> "\"" + t + "\"").toArray(String[]::new));
        try (Connection conn = ConnectionPool.getConnection()) {
            assertDoesNotThrow(() ->
                    ConnectionPool.execute(conn, "SELECT %s FROM (%s) LIMIT 0".formatted(tokenList, wrapped)),
                    "Every bucket PARTITION_BY token must resolve as a column in the projected relation");
        }
    }

    /**
     * The projected {@code _dd_iceberg_bucket(col, N)} must equal DuckLake's native {@code bucket()}
     * — otherwise files register under the wrong partition and pruning silently drops rows. Ground
     * truth was captured from DuckLake 1.5.5's on-disk {@code bucket=<v>} directories (N=16) across
     * the long domain: 0, positives, snowflake ids, {@code Long.MIN/MAX_VALUE}, negatives, and the
     * int32 boundaries (int columns hash as promoted longs).
     */
    @Test
    void icebergBucketMacroMatchesDuckLakeNativeBucket() throws Exception {
        // Resolving a bucket-partitioned table registers the macros (lazily, on first bucket use).
        handlerForPartitionedTable("macro_bootstrap", "bucket(4, id)", "id BIGINT")
                .getPartitionProjections("macro_bootstrap");

        String probe = """
                SELECT count(*) FILTER (WHERE truth <> _dd_iceberg_bucket(g, 16)) AS mismatches
                FROM (VALUES (0,12),(1,4),(2,4),(3,3),(7,3),(11,7),(42,14),(255,7),(1000000,6),
                             (123456789,1),(1000000000000,12),(361193724270612480,9),
                             (361193725126246400,5),(9223372036854775807,15),
                             (-1,8),(-2,5),(-100,14),(2147483647,14),(-2147483648,8),
                             (-9223372036854775808,5)) v(g, truth)
                """;
        try (Connection conn = ConnectionPool.getConnection()) {
            long mismatches = ConnectionPool.collectFirst(conn, probe, Long.class);
            assertEquals(0L, mismatches,
                    "_dd_iceberg_bucket must match DuckLake's native bucket(16, ...) for every probe value");
        }
    }

    /**
     * Stronger than the static probe above: compares the macro to DuckLake's <em>own</em> bucketing
     * computed at test time. Inlining is disabled so each row lands in a file whose hive dir carries
     * the native bucket value; the macro must reproduce it for every value and sign.
     */
    @Test
    void icebergBucketMatchesDuckLakeNativeComputedAtTestTime() throws Exception {
        String table = "native_bucket_probe";
        handlerForPartitionedTable("macro_bootstrap2", "bucket(4, id)", "id BIGINT")
                .getPartitionProjections("macro_bootstrap2");   // register the macros

        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn,
                    "CALL %s.set_option('data_inlining_row_limit', 0)".formatted(CATALOG));
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "CREATE TABLE %s.%s.%s (g BIGINT)".formatted(CATALOG, SCHEMA, table),
                    "ALTER TABLE %s.%s.%s SET PARTITIONED BY (bucket(16, g))".formatted(CATALOG, SCHEMA, table),
                    ("INSERT INTO %s.%s.%s VALUES (0),(1),(42),(255),(-1),(-2),(-100),"
                            + "(2147483647),(-2147483648),(9223372036854775807),(-9223372036854775808),"
                            + "(361193724270612480)").formatted(CATALOG, SCHEMA, table)
            });
            String dataGlob = tempDir.resolve("data").resolve(SCHEMA).resolve(table)
                    .resolve("**").resolve("*.parquet").toString();
            String cmp = ("SELECT count(*) FROM ("
                    + "SELECT g, _dd_iceberg_bucket(g, 16) AS macro, "
                    + "regexp_extract(filename, 'bucket=([0-9]+)', 1)::BIGINT AS native "
                    + "FROM read_parquet('%s', filename=true)) WHERE macro <> native").formatted(dataGlob);
            long mismatches = ConnectionPool.collectFirst(conn, cmp, Long.class);
            assertEquals(0L, mismatches,
                    "_dd_iceberg_bucket must equal DuckLake's on-disk native bucket for every value/sign");
        }
    }

    @Test
    void shouldAcceptBucketOnInt32Column() throws Exception {
        // Iceberg promotes int32 to a long for hashing, so the long-based macro is correct.
        var factory = handlerForPartitionedTable("int32_bucket_events", "bucket(4, uid)", "uid INTEGER");
        assertArrayEquals(new String[]{"bucket"}, factory.getPartitionBy("int32_bucket_events"));
        assertArrayEquals(new String[]{"_dd_iceberg_bucket(\"uid\", 4) AS \"bucket\""},
                factory.getPartitionProjections("int32_bucket_events"));
    }

    @Test
    void shouldRejectBucketOnNonIntegerColumn() throws Exception {
        // DuckLake accepts bucket() on a varchar column, but Iceberg hashes its bytes differently;
        // the collector must fail fast with a clear message rather than emit a wrong/failing COPY.
        String table = "bad_bucket_events";
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "CREATE TABLE %s.%s.%s (id BIGINT, name VARCHAR)".formatted(CATALOG, SCHEMA, table),
                    "ALTER TABLE %s.%s.%s SET PARTITIONED BY (bucket(4, name))".formatted(CATALOG, SCHEMA, table)
            });
        }
        var mapping = new QueueIdToTableMapping(table, CATALOG, SCHEMA, table, Map.of(), null);
        // Partitions resolve eagerly on construction, so the unsupported type surfaces there.
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new DuckLakeIngestionHandler(Map.of(table, mapping)));
        assertTrue(ex.getMessage().toLowerCase().contains("integer"),
                "message must explain the integer-only limitation, was: " + ex.getMessage());
    }

    @Test
    void shouldDisambiguateCollidingTimeTransforms() throws Exception {
        // Two day() transforms collide on the "day" key; DuckLake names them day / day_b, and the
        // handler must match so ducklake_add_data_files accepts the files.
        var factory = handlerForPartitionedTable("dual_day_events", "day(a), day(b)",
                "a TIMESTAMP, b TIMESTAMP");
        assertArrayEquals(new String[]{"day", "day_b"}, factory.getPartitionBy("dual_day_events"));
        assertArrayEquals(new String[]{"day(\"a\") AS \"day\"", "day(\"b\") AS \"day_b\""},
                factory.getPartitionProjections("dual_day_events"));
    }
}
