package io.dazzleduck.sql.flight.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dazzleduck.sql.common.UnauthorizedException;
import io.dazzleduck.sql.common.authorization.AccessRow;
import io.dazzleduck.sql.common.authorization.SimpleAuthorization;
import io.dazzleduck.sql.common.authorization.SqlAuthorizer;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.util.TestConstants;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class AccessControlTest {

    String supportedTableQuery = "SELECT a, b, c from test_table where filter1 and filter2";
    String filter = "key = 'k1'";
    AccessRow pathAccessRow = new AccessRow("test_group", null, null, "example/hive_table/*/*/*.parquet", Transformations.TableType.TABLE_FUNCTION, List.of(), filter, new Date(System.currentTimeMillis() + Duration.ofHours(1).toMillis()), "read_parquet");
    AccessRow tableAccessRow = new AccessRow("test_group", "test_db", "test_schema", "test_table", Transformations.TableType.BASE_TABLE, List.of(), filter, new Date(System.currentTimeMillis() + Duration.ofHours(1).toMillis()), null);
    SqlAuthorizer sqlAuthorizer = new SimpleAuthorization(Map.of("test_user", List.of("test_group")),
            List.of(pathAccessRow, tableAccessRow));

    String aggregateSql(String inner) {
        return "SELECT key, count(*), count(distinct value) FROM (%s) GROUP BY key".formatted(inner);
    }

    @Test
    public void readMissingColumn() {
        ConnectionPool.printResult(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
    }

    @Test
    public void testAggregation() {
        var aggregateSql = aggregateSql(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
        ConnectionPool.printResult(aggregateSql);
    }

    @Test
    public void rowLevelFilterForPath() throws SQLException, JsonProcessingException, UnauthorizedException {
        var query = Transformations.parseToTree(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
        var authorizedQuery = sqlAuthorizer.authorize("test_user", null, null, query);
        var result = Transformations.parseToSql(authorizedQuery);
        ConnectionPool.execute(result);
    }

    @Test
    public void rowLevelFilterForPathAggregation() throws SQLException, JsonProcessingException, UnauthorizedException {
        var aggregateSql = aggregateSql(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
        var query = Transformations.parseToTree(aggregateSql);
        var authorizedQuery = sqlAuthorizer.authorize("test_user", null, null, query);
        var result = Transformations.parseToSql(authorizedQuery);
        ConnectionPool.execute(result);
    }

    @Test
    public void rowLevelFilterForTable() throws SQLException, JsonProcessingException, UnauthorizedException {
        var query = Transformations.parseToTree(supportedTableQuery);
        var authorizedQuery = sqlAuthorizer.authorize("test_user", "test_db", "test_schema", query);
        Transformations.parseToSql(authorizedQuery);
    }

    @Test
    public void rowLevelFilterForTableAggregation() throws SQLException, JsonProcessingException, UnauthorizedException {
        var aggregateSql = aggregateSql(supportedTableQuery);
        var query = Transformations.parseToTree(aggregateSql);
        var authorizedQuery = sqlAuthorizer.authorize("test_user", "test_db", "test_schema", query);
        Transformations.parseToSql(authorizedQuery);
    }
}
