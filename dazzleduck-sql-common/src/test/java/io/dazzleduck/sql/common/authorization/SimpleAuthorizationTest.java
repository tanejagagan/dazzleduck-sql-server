package io.dazzleduck.sql.common.authorization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.UnauthorizedException;
import io.dazzleduck.sql.common.authorization.AccessRow;
import io.dazzleduck.sql.common.authorization.SimpleAuthorization;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.Transformations.CatalogSchemaTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Date;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class SimpleAuthorizationTest {

    static Stream<Arguments> provideParametersForPaths() {
        return Stream.of(
                Arguments.of("select * from read_parquet('/x/y/z/abc.parquet')", "/x/y/z/abc.parquet", "TABLE_FUNCTION"),
                Arguments.of("select * from read_json('abc.json')", "abc.json", "TABLE_FUNCTION"),
                Arguments.of("select * from read_csv('abc.csv')", "abc.csv", "TABLE_FUNCTION")
        );
    }

    static Stream<Arguments> provideParametersForTables() {
        return Stream.of(
                Arguments.of("select * from a", "a", "BASE_TABLE")
        );
    }

    @Test
    public void combineFilterTest() throws SQLException, JsonProcessingException {
        String sql = "select * from t1 where a = b";
        String filterToCombiner = "select * from t1 where x = y";
        var query = Transformations.parseToTree(sql);
        var toAddQuery = Transformations.parseToTree(filterToCombiner);
        var toAddFilter = Transformations.getWhereClause(toAddQuery);
        var newQuery = SimpleAuthorization.addFilerToQuery(query, toAddFilter);
        var newQueryString = Transformations.parseToSql(newQuery);
        assertTrue(newQueryString.contains("x") && newQueryString.contains("y"));
    }

    @Test
    public void combineFilterTestNoFilter() throws SQLException, JsonProcessingException {
        String sql = "select * from t1";
        String filterToCombiner = "select * from t1 where x = y";
        var query = Transformations.parseToTree(sql);
        var toAddQuery = Transformations.parseToTree(filterToCombiner);
        var toAddFilter = Transformations.getWhereClause(toAddQuery);
        var newQuery = SimpleAuthorization.addFilerToQuery(query, toAddFilter);
        var newQueryString = Transformations.parseToSql(newQuery);
        assertTrue(newQueryString.contains("x") && newQueryString.contains("y"));
    }

    @ParameterizedTest()
    @ValueSource(strings = {
            "select * from read_parquet('abc')",
            "select * from read_delta('abc')"
    })
    public void validateForAuthorizationPositiveTest(String sql) throws SQLException, JsonProcessingException, UnauthorizedException {
        JsonNode parsedQuery = Transformations.parseToTree(sql);
        SimpleAuthorization.validateForAuthorization(parsedQuery);
    }

    @ParameterizedTest()
    @ValueSource(strings = {
            "with x as ( select * from generate_series(10)  where x = y) select * from x",
            "select * from a ,b ",
            "select * from read_parquet('abc') where a in (select name from b)",
            "select * from read_parquet('abc') where a in (select name from b) and c = d",
            "select * from read_parquet('abc') where a in (select name from b) or c = d"
    })
    public void validateForAuthorizationFailureTest(String sql) throws SQLException, JsonProcessingException, UnauthorizedException {
        JsonNode parsedQuery = Transformations.parseToTree(sql);
        var thrown = assertThrows(
                UnauthorizedException.class,
                () -> SimpleAuthorization.validateForAuthorization(parsedQuery),
                "Expected doThing() to throw, but it didn't");
    }

    @Test
    public void testLoad() throws SQLException, JsonProcessingException, UnauthorizedException {
        var userGroupMapping = Map.of("u1", List.of("g1", "g2"));
        var accessRows = List.of(
                new AccessRow("g1", "d1", "s1", "t1", "BASE_TABLE", List.of(), "a = 1", new Date(0)),
                new AccessRow("g2", "d1", "s1", "t1", "BASE_TABLE", List.of(), "a = 2", new Date(0)));
        var s = new SimpleAuthorization(userGroupMapping, accessRows);
        var q = Transformations.parseToTree("select * from t1  where t = x");
        var result = s.authorize("u1", "d1", "s1", q);
        var sql = Transformations.parseToSql(result);
        assertEquals("SELECT * FROM t1 WHERE ((t = x) AND ((a = 2) OR (a = 1)))", sql);
    }

    @ParameterizedTest
    @MethodSource("provideParametersForPaths")
    void testGetTableForPaths(String sql, String expectedTablePath, String tableType) throws SQLException, JsonProcessingException {
        assertEquals(new Transformations.CatalogSchemaTable(null, null, expectedTablePath, tableType),
                Transformations.getTableOrPath(Transformations.parseToTree(sql), "c", "s"));
    }

    @ParameterizedTest
    @MethodSource("provideParametersForTables")
    void testGetTableForTable(String sql, String expectedPath, String tableType) throws SQLException, JsonProcessingException {
        assertEquals(new CatalogSchemaTable("c", "s", expectedPath, tableType),
                Transformations.getTableOrPath(Transformations.parseToTree(sql), "c", "s"));
    }

    @Test
    public void testDDL() throws SQLException, JsonProcessingException {
        var sql = "select null::struct( a string, b struct ( x string)) union all select a ";
        var tree = Transformations.parseToTree(sql);
        System.out.println(tree.toPrettyString());
    }
}
