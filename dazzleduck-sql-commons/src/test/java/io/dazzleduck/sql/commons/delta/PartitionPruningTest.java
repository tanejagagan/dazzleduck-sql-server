package io.dazzleduck.sql.commons.delta;


import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static io.dazzleduck.sql.commons.delta.PartitionPruning.*;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class PartitionPruningTest {
    static final String basePath = "example/delta_table";
    static final String[][] partition = {{"dt", "date"}, {"p", "string"}};

    private static void assertSize(int expectedSize, String basePath, String filter) throws IOException, SQLException {
        List<?> result = pruneFiles(basePath, filter, partition);
        assertEquals(expectedSize, result.size(), result.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    @Test
    @Disabled("Requires Hadoop 3.4.3+ for Java 23+ compatibility (HADOOP-19212)")
    public void pruneFilesPartitionTest() throws SQLException, IOException {
        /*
         * +---+-----+----------+---+-------------------------------------------------------------------+
         * |key|value|dt        |p  |filename                                                           |
         * +---+-----+----------+---+-------------------------------------------------------------------+
         * |k2 |v1   |2025-03-01|b  |part-00000-852ce60c-3eb1-4fe0-96e9-973ee178fe17.c000.snappy.parquet|
         * |k3 |v1   |2024-11-01|c  |part-00001-1567ddf3-190d-409b-9f26-94f259402fd9.c000.snappy.parquet|
         * |k2 |v2   |2024-05-01|c  |part-00002-207791f6-872c-49e3-9b8f-abb1bbe15a24.c000.snappy.parquet|
         * |k3 |v3   |2024-01-01|b  |part-00003-92c171de-4689-497a-b48a-97b5c6173f1b.c000.snappy.parquet|
         * |k4 |v4   |2024-01-01|y  |part-00004-bc9896f8-f5a1-4062-8ca2-4c515012bdb1.c000.snappy.parquet|
         * |k1 |v1   |2025-01-01|a b|part-00005-1759e6d9-bab3-4fc9-9f5c-191089d8f953.c000.snappy.parquet|
         * |k2 |v2   |2025-01-01|a b|part-00005-1759e6d9-bab3-4fc9-9f5c-191089d8f953.c000.snappy.parquet|
         * |k1 |v1   |2024-01-01|x  |part-00006-82995fce-3d25-4a44-a4bc-5b1bdd1e27e9.c000.snappy.parquet|
         * |k2 |v2   |2024-01-01|x  |part-00006-82995fce-3d25-4a44-a4bc-5b1bdd1e27e9.c000.snappy.parquet|
         * |k1 |v1   |2025-01-01|1  |part-00007-1bb083f8-5385-48fe-8257-4f5510e681b1.c000.snappy.parquet|
         * |k2 |v2   |2025-01-01|1  |part-00007-1bb083f8-5385-48fe-8257-4f5510e681b1.c000.snappy.parquet|
         * +---+-----+----------+---+-------------------------------------------------------------------+
         */
        // pruneFiles with no partition column filters
        assertSize(5, basePath, "value='v1'");
        assertSize(4, basePath, "value='v2'");
        assertSize(6, basePath, "value='v1' OR value='v2'");
        assertSize(3, basePath, "value='v1' AND value='v2'");
        assertSize(4, basePath, "value='v1' AND key='k2'");
        assertSize(6, basePath, "value='v1' OR key='k2'");

        // pruneFiles with partition column filters
        assertSize(3, basePath, "dt=cast('2024-01-01' AS date)");
        assertSize(1, basePath, "p='x'");
        assertSize(1, basePath, "dt=cast('2024-01-01' AS date) AND p = 'x'");
        assertSize(2, basePath,"p='a b' OR p ='x'");
        assertSize(0, basePath, "p='abc'");
        assertSize(0, basePath, "dt=cast('2024-01-01' as date) AND p='abc'");


        // pruneFiles with partition column and no partition column filters
        assertSize(1, basePath, "dt=cast('2024-01-01' as date) AND value='v1'");
        assertSize(1, basePath, "dt=cast('2024-01-01' as date) AND value='v1' and value='v2'");
        assertSize(8, basePath, "dt=cast('2024-01-01' as date) OR (value='v1' OR value='v2')");
        assertSize(0, basePath, "dt=cast('2024-01-01' as date) AND value='v1' AND p='abc'");

       // TODO
       // 1. assertSize(3, basePath, "value IN ('v1', 'v2')");
       // java.lang.UnsupportedOperationException: No transformation supported{"class":"OPERATOR","type":"COMPARE_IN"

       // 2. Pruning files using partition column filters is a best-effort approach by the Delta kernel
       // and may not always achieve full optimization.
       // assertSize(3, basePath, "value='v123'"); // expect to return 0
    }
}