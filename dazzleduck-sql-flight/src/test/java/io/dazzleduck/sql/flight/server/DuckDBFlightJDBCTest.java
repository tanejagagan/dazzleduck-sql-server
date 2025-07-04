package io.dazzleduck.sql.flight.server;


import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.*;

public class DuckDBFlightJDBCTest {

    private static Location serverLocation = Location.forGrpcInsecure("localhost", 55556);

    private static final String LONG_RUNNING_QUERY = "with t as " +
            "(select len(split(concat('abcdefghijklmnopqrstuvwxyz:', generate_series), ':')) as len  from generate_series(1, 1000000000) )" +
            " select count(*) from t where len = 10";
    private static FlightServer flightServer ;
    @BeforeAll
    public static void beforeAll() throws IOException {
        flightServer = FlightServer.builder(
                        new RootAllocator(),
                        serverLocation,
                        new DuckDBFlightSqlProducer(serverLocation))
                .build()
                .start();
    }

    @AfterAll
    public static void afterAll() throws InterruptedException {
        flightServer.close();
    }

    private static Connection getConnection() throws SQLException {
        String url = String.format("jdbc:arrow-flight-sql://localhost:%s/?database=memory&useEncryption=0", flightServer.getPort());
        return DriverManager.getConnection(url);
    }

    private static Connection getConnectionWithUserPassword() throws SQLException {
        String url = String.format("jdbc:arrow-flight-sql://localhost:%s/?database=memory&useEncryption=0&user=admin&password=pass", flightServer.getPort());
        return DriverManager.getConnection(url);
    }

    @Test
    public void testExecuteQuery() throws SQLException {
        try(Connection connection = getConnection();
            Statement  st = connection.createStatement()) {
            st.executeQuery("select 1");
            try ( ResultSet resultSet = st.getResultSet()) {
                while (resultSet.next()){
                    resultSet.getInt(1);
                }
            };
        }
    }

    @Test
    public void cancelBeforeExecute() throws SQLException {
        try(Connection connection = getConnection();
            Statement  st = connection.createStatement()) {
            st.cancel();
        }
    }

    @Test
    public void testExecuteQueryWithUserPassword() throws SQLException {
        try(Connection connection = getConnectionWithUserPassword();
            Statement  st = connection.createStatement()) {
            st.executeQuery("select 1");
            try ( ResultSet resultSet = st.getResultSet()) {
                while (resultSet.next()){
                    resultSet.getInt(1);
                }
            };
        }
    }

    @Test
    @Disabled // This is not working because some jdbc issue.
    public void cancelAfterExecute() throws SQLException {
        try(Connection connection = getConnection();
            Statement  st = connection.createStatement()) {
            Thread cancelThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        // Cancel after 1 second.
                        Thread.sleep(1000);
                        st.cancel();
                    } catch (SQLException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            cancelThread.start();
            st.execute(LONG_RUNNING_QUERY);
            st.cancel();
        }
    }


    @Test
    @Disabled
    public void testSetUnSet() throws SQLException {
        try(Connection connection = getConnection();
            Statement  st = connection.createStatement()) {
            st.executeUpdate("set pivot_limit=9999");
            st.execute("SELECT current_setting('pivot_limit') AS memlimit");
            try(ResultSet set  = st.getResultSet()) {
                while (set.next()){
                    System.out.println(set.getObject(1, Long.class));
                }
            }
        }
    }

    @Test
    // These are disabled because the issue with driver which
    // does not let me troubleshoot the issue as it's a uber jar
    // and lines do not match.
    public void testMetadata() throws SQLException {
        try(Connection connection = getConnection()) {
            var databaseMetadata = connection.getMetaData();
            //databaseMetadata.getTables();
            try (var rs = databaseMetadata.getSchemas()) {
                while(rs.next()) {
                    System.out.printf("%s, %s\n",
                            rs.getString(1),
                            rs.getString(2));
                }
            }

            //databaseMetadata.getCatalogs();
            try (var rs = databaseMetadata.getCatalogs()) {
                while(rs.next()) {
                    System.out.printf("%s\n",
                            rs.getString(1));
                }
            }

            try( var rs = databaseMetadata.getTables(null, null, null, null)){
                while(rs.next()) {
                    System.out.printf("%s",
                            rs.getString(1));
                }
            }
        }
    }
}
