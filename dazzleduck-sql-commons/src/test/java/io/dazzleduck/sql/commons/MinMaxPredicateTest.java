package io.dazzleduck.sql.commons;

import io.dazzleduck.sql.commons.util.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class MinMaxPredicateTest {
    public static int numRows = 20;
    public static int rowsPerPartition = 5;
    public static String TEST_TABLE_NAME = "test_min_max_pruning";
    @BeforeAll
    public static void startup(){
        String tableCreate = "CREATE TABLE %s (key int, p int, min_key varchar, max_key varchar)".formatted(TEST_TABLE_NAME);
        ConnectionPool.execute(tableCreate);
        for(int i = 0 ; i < numRows; i ++) {
            var p = i/rowsPerPartition;
            var min = p * rowsPerPartition;
            var max = min + rowsPerPartition - 1;
            var s = "INSERT INTO %s VALUES (%s, %s, '%s', '%s')".formatted(TEST_TABLE_NAME, i,p, min, max);
            ConnectionPool.execute(s);
        }
    }

    @AfterAll
    public static void cleanup(){
        ConnectionPool.execute("DROP TABLE %s".formatted(TEST_TABLE_NAME));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select distinct p from test_min_max_pruning where key = 12 order by p",
            "select distinct p from test_min_max_pruning where key < 12 order by p",
            "select distinct p from test_min_max_pruning where key > 12 order by p"
    })
    public void testReplaceMinMax(String query) throws SQLException, IOException {
        var minMapping = Map.of("key",  "min_key");
        var maxMapping = Map.of("key",  "max_key");
        var dataTypeMapping = Map.of("key",  "INTEGER");
        var t = Transformations.replaceEqualMinMaxInQuery("test_min_max_pruning", minMapping, maxMapping, dataTypeMapping );
        var tree = Transformations.parseToTree(query);
            var transformed = t.apply(tree);
            var transformedSql = Transformations.parseToSql(transformed);
            TestUtils.isEqual(query, transformedSql);
    }
}
