package io.dazzleduck.sql.commons.benchmark;

import io.dazzleduck.sql.commons.ConnectionPool;

import java.sql.Connection;
import java.sql.SQLException;

public class BenchmarkUtil {

    public static void createBenchmarkData(String benchmark,
                                            String genFun,
                                            String scaleFactor,
                                            String dataPathStr) throws SQLException {
        String[] sqls = {
                String.format("INSTALL %s", benchmark),
                String.format("LOAD %s", benchmark),
                String.format("CALL %s(sf = %s)", genFun, scaleFactor)
        };
        try (Connection connection  = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatch(connection, sqls);

            // Copy data to parquet files
            for (String table : ConnectionPool.collectFirstColumn(connection, "show tables", String.class)) {
                copyData(connection, table,
                        String.format("%s/%s.parquet", dataPathStr, table));
            }
        }
    }

    private static void copyData(Connection connection, String srcTable, String destPath) throws SQLException {
        System.out.printf("COPYING parquet files : %s, %s%n", srcTable, destPath);
        var sql = String.format("COPY (SELECT * FROM %s) TO '%s' (FORMAT 'parquet');", srcTable, destPath);
        ConnectionPool.execute(connection, sql);
    }
}
