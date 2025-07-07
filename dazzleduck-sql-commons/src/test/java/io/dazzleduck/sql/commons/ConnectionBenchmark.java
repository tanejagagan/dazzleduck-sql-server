package io.dazzleduck.sql.commons;

import io.dazzleduck.sql.commons.benchmark.BenchmarkUtil;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ConnectionBenchmark {

    public static void main(String args[]) throws Exception {
        final int numThreads = 8;
        final int iteration = 400;
        String benchmark = "tpcds";
        String genFun = "dsdgen";
        String scaleFactor = "1";
        String outputPath = String.format("/tmp/%s_sf%s", benchmark, scaleFactor );

        var path = Paths.get(outputPath);
        if(!Files.exists(path)) {
            System.out.println("Creating dir: " + path );
            Files.createDirectories(path);
            BenchmarkUtil.createBenchmarkData(benchmark, genFun, scaleFactor, outputPath);
        }

        String sql = String.format("select count(distinct ss_customer_sk) from read_parquet('%s/store_sales.parquet')", outputPath);

        // Running it for 100 times in single thread
        time(() -> {
            for (int i = 0; i < iteration; i++) {
                ConnectionPool.execute(sql);
            }
            return Void.TYPE;
        });

        benchmark(numThreads, iteration, sql);
    }

    private static void benchmark(int numThreads, int iteration, String sql ) {
        ExecutorService executors = Executors.newFixedThreadPool(10);
        time(() -> {
            for (int i = 0; i < numThreads; i++) {
                for (int j = 0; j < iteration; j++) {
                    executors.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                DuckDBConnection connection = ConnectionPool.getConnection();
                                execute(connection.duplicate(), sql);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                }
            }
            executors.shutdown();
            try {
                executors.awaitTermination(100, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return Void.TYPE;
        });
    }

    public static void execute(Connection connection, String sql) {
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
            try(DuckDBResultSet rs = (DuckDBResultSet) st.getResultSet()) {
                while (rs.next()) {
                    rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T time(Supplier<T> supplier) {
        long start = System.currentTimeMillis();
        T result =  supplier.get();
        long end = System.currentTimeMillis();
        System.out.println("Total time :" + (end -start));
        return result;
    }
}
