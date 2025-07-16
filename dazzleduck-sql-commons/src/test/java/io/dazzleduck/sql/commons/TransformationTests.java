package io.dazzleduck.sql.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TransformationTests {

    @Test
    public void testSplitStatements() throws SQLException, JsonProcessingException {
        String sql = " select * from generate_series(10); \n" +
                "select * from generate_series(11);";
        JsonNode node = Transformations.parseToTree(sql);
        List<JsonNode> statements = Transformations.splitStatements(node);
        for(JsonNode n : statements) {
            String s = Transformations.parseToSql(n);
            ConnectionPool.execute(s);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"select * from t where x in (select y from t2) "})
    public void testSubQueries(String sql) throws SQLException, JsonProcessingException {
        var jsonNode = Transformations.parseToTree(sql);
        var statements = (ArrayNode) jsonNode.get("statements");
        var statement = statements.get(0);
        var statementNode = statement.get("node");
        var where = statementNode.get("where_clause");
        var qs = Transformations.collectSubQueries(where );
        Assertions.assertEquals(1, qs.size());
    }

    @Test
    public void getCast() throws SQLException, JsonProcessingException {
        var schema = "a int, b string, c STRUCT(i  int, d STRUCT( x int)), e Int[], f Map(string, string), g decimal(18,3)";
        var sql = Transformations.getCast(schema);
        ConnectionPool.printResult("select " + sql);
    }

    @Test
    public void getPartitionSchema() throws SQLException, JsonProcessingException {
        var query = "select * from read_parquet('abc', hive_types = {a : INT, b : STRING})";
        var hivePartition = Transformations.getHivePartition(Transformations.parseToTree(query));
        assertEquals(2, hivePartition.length);
    }
}
