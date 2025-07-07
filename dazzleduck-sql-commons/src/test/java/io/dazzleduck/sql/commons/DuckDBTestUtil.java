package io.dazzleduck.sql.commons;

import io.dazzleduck.sql.commons.util.TestUtils;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.duckdb.DuckDBConnection;
import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class DuckDBTestUtil {


    public static void testMappedReader(String sql,
                                        MappedReader.Function function,
                                        List<String> sourceCol,
                                        Field targetField,
                                        String tempTableName,
                                        String testSql,
                                        String expectedSql) throws SQLException, IOException {
        try (DuckDBConnection readConnection = ConnectionPool.getConnection();
             DuckDBConnection writeConnection = ConnectionPool.getConnection();
             RootAllocator allocator = new RootAllocator();
             ArrowReader reader = ConnectionPool.getReader(readConnection, allocator, sql, 10);
             Closeable ignored = ConnectionPool.createTempTableWithMap(writeConnection, allocator, reader, function, sourceCol, targetField, tempTableName)) {
            // the issue with temp tables for now are that they can be only be queries once since it reads the reader once.
            // It does not work even for queries such as `select * from a union select * from a` because it will require reader to be read twice.
            // Therefor we need to store the data of reader into materialized view so that it can be used multiple time
            String materializedTable = tempTableName + "_mat";
            ConnectionPool.execute(writeConnection, String.format("CREATE TABLE %s AS SELECT * FROM %s", materializedTable, tempTableName));
            TestUtils.isEqual(writeConnection, allocator, testSql.replaceAll(tempTableName, materializedTable), expectedSql);
        }
    }

}
