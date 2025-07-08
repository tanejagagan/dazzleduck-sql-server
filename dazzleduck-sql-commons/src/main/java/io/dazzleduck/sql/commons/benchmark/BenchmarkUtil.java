package io.dazzleduck.sql.commons.benchmark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.dazzleduck.sql.commons.ConnectionPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

public class BenchmarkUtil {

    public static void main(String []args  ) throws SQLException, IOException {
        var argv = new Args();
        JCommander.newBuilder()
                .addObject(argv)
                .build()
                .parse(args);
        var genFun = argv.benchmarkName.equals("tpcds") ? "dsdgen" : "dbgen";
        createBenchmarkData(argv.benchmarkName, genFun, argv.scaleFactor, argv.path);
    }

    public static class Args {
        @Parameter(names = "--path", description = "Location where files will be written")
        String path;

        @Parameter(names = {"--sf", "--scale_factor"}, description = "scale factor")
        String scaleFactor;

        @Parameter(names = {"--name"}, description = "Name of the benchmark [tpcdds, tpc]")
        String benchmarkName;

    }

    public static void createBenchmarkData(String benchmark,
                                           String genFun,
                                           String scaleFactor,
                                           String dataPathStr) throws SQLException, IOException {
        Files.createDirectories(Path.of(dataPathStr));
        String[] sqls = {
                String.format("INSTALL %s", benchmark),
                String.format("LOAD %s", benchmark),
                String.format("CALL %s(sf = %s)", genFun, scaleFactor)
        };
        try (Connection connection  = ConnectionPool.getConnection()) {
            System.out.println("Executing generation");
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
